import requests
import hashlib
import os
import git
import json
import diff2HtmlCompare
from tqdm import tqdm
from bs4 import BeautifulSoup
from pdf_diff import command_line as pdf
from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


def auth():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def get_last_modified(url):
    return requests.head(url).headers['last-modified']


def get_file_hash(file):
    with open(file, 'rb') as toHash:
        return hashlib.md5(toHash.read()).hexdigest()


def get_file_name(url):
    ext = "" if url[-3:].lower() == "pdf" else ".html"
    return url.split("/")[-1] + ext


def save_file(url, tmp=False):
    filename = get_file_name(url)
    if tmp:
        file = "tmp_" + filename
    else:
        file = FILE_STORE + filename

    is_pdf = True if filename[-3:].lower() == 'pdf' else False
    mode = 'wb' if is_pdf else 'w'

    content = requests.get(url).content

    if is_pdf:
        with open(file, 'wb') as out:
            out.write(content)
    else:
        soup = BeautifulSoup(content, features="lxml")
        content = "\n".join(item for item in soup.find(id='content').get_text().split('\n') if item)

        with open(file, 'w', encoding='utf8') as out:
            out.write(content)


def save_and_log(url):
    save_file(url)
    name = get_file_name(url)
    modified = get_last_modified(url)
    hash_val = get_file_hash(FILE_STORE + name)

    return {'modified': modified, 'hash': hash_val}


def build_metadata(urls):
    with open(META_FILE, 'w') as outfile:
        data = {}
        for url in urls:
            name = get_file_name(url)
            data[name] = save_and_log(url)
        json.dump(data, outfile, indent=4)


def read_metadata():
    with open(META_FILE) as infile:
        return json.load(infile)


def save_metadata(data):
    with open(META_FILE, 'w') as outfile:
        json.dump(data, outfile, indent=4)


def save_log(message):
    date_string = datetime.today().strftime('%Y-%m-%d')
    if not os.path.exists(LOG_STORE):
        os.makedirs(LOG_STORE)
    log_file = LOG_STORE + date_string + '.txt'
    with open(log_file, 'w') as out:
        out.write(message)


def get_date_folder():
    date_string = datetime.today().strftime('%Y-%m-%d')
    folder_name = date_string + '/'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name


def get_item_folder(filename):
    folder_name = SITE_STORE + filename.split('/')[-1].split('.')[0] + '/'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name


def process_data_file(url, old_hash):
    save_file(url, tmp=True)
    filename = get_file_name(url)
    tmp_name = 'tmp_' + filename

    new_hash = get_file_hash(tmp_name)

    if new_hash != old_hash:
        # Hashes don't match, something changed so keep the file and diff
        day_folder = get_date_folder()
        old_file = 'old/' + day_folder + filename
        current_file = FILE_STORE + filename

        # Move the old file copy into the archive for today, move the new copy into the store
        os.rename(current_file, old_file)
        os.rename(tmp_name, current_file)

        is_pdf = True if filename[-3:].lower() == 'pdf' else False
        if is_pdf:
            diff_pdf(old_file, current_file)
        else:
            diff_html(old_file, current_file)
    else:
        # Hashes match, just delete the new file
        os.remove(tmp_name)

    return new_hash


def diff_pdf(old_file, new_file):
    changes = pdf.compute_changes(old_file, new_file)
    img = pdf.render_changes(changes, ['strike', 'underline', 'box'], 1920)
    folder = get_item_folder(new_file)
    date_string = datetime.today().strftime('%Y-%m-%d')
    img.save(folder + date_string + '.png')


def diff_html(old_file, new_file):
    folder = get_item_folder(new_file)
    date_string = datetime.today().strftime('%Y-%m-%d')
    full_out = folder + date_string + '.html'
    class C:
        pass
    c = C()
    c.verbose = False
    c.show = False
    c.print_width = False
    c.syntax_css = 'vs'
    diff2HtmlCompare.main(old_file, new_file, full_out, c)


def upload_diffs():
    ret = 'Git: Successfully pushed. \n'
    try:
        repo = git.Repo('.')
        for file in repo.untracked_files:
            if file.split('/')[0] == 'out':
                repo.git.add(file)
        repo.git.add(update=True)
        date_string = datetime.today().strftime('%Y-%m-%d')
        message = 'Changes from ' + date_string
        repo.git.commit(m=message)
        origin = repo.remote(name='origin')
        origin.push()
    except:
        ret = '### ERROR: GIT FAILED \n'

    return ret


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SHEET_ID = '1PWxwiKkbzossw06pUPkmJMkLj9r1dRJ0v2GQ_1cITzQ'
DATA_RANGE = 'Data!A2:A'
FILE_STORE = 'store/'
SITE_STORE = 'out/'
LOG_STORE = 'logs/'
EPOCH = 'Thu, 01 Jan 1970 00:00:00 GMT'
META_FILE = 'status.json'


def main():
    sheets = build('sheets', 'v4', credentials=auth()).spreadsheets().values()
    urls = [url[0] for url in sheets.get(spreadsheetId=SHEET_ID, range=DATA_RANGE).execute().get('values', [])]

    metadata = read_metadata()
    meta_changed = False
    files_changed = False
    log_text = ''

    for url in tqdm(urls):
        possible_change = False

        filename = get_file_name(url)

        # Try / except to catch possible 404s or other issues.
        try:
            modified = get_last_modified(url)

            if filename not in metadata:
                # New file that hasn't been processed before, add it to metadata
                metadata[filename] = save_and_log(url)
                print('Added:' + filename)
                log_text += 'Added: ' + filename + '\n'
                meta_changed = True

            elif modified == EPOCH:
                # Bad last-modified date, check via hashes
                possible_change = True

            elif modified == metadata[filename]['modified']:
                # File hasn't been modified since last checked, skip it.
                pass

            else:
                # last-modified has changed
                metadata[filename]['modified'] = modified
                meta_changed = True
                possible_change = True

            # In all cases where there might be a file change download the file and check the hash. If there is a
            # change then move the old file into an archive and move the new file into the store.
            if possible_change:
                old_hash = metadata[filename]['hash']
                new_hash = process_data_file(url, old_hash)

                if new_hash != old_hash:
                    print("Updated: " + filename)
                    log_text += 'Change: ' + filename + '\n'
                    metadata[filename]['hash'] = new_hash
                    meta_changed = True
                    files_changed = True
        except:
            log_text += '### ERROR: ' + filename.upper()
            pass

    # only write metadata if there has been at least one change.
    if meta_changed:
        save_metadata(metadata)
    else:
        log_text += 'No changes detected. \n'
        print("No changes detected.")

    if files_changed:
        log_text += upload_diffs()

    save_log(log_text)


if __name__ == '__main__':
    main()
