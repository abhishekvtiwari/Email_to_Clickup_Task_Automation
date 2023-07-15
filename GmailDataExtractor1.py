import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import yaml
import re
import imaplib
import email
import datetime
from git import Repo


class GmailMailer:
    def __init__(self, credentials_file_path, mail_send_list_file_path, repo_url, filtered_email_data_file,
                 matched_email_data_file, original_email_data_file, sent_email_data_file):
        self.credentials_file_path = credentials_file_path
        self.mail_send_list_file_path = mail_send_list_file_path
        self.repo_url = repo_url
        self.filtered_email_data_file = filtered_email_data_file
        self.matched_email_data_file = matched_email_data_file
        self.original_email_data_file = original_email_data_file
        self.sent_email_data_file = sent_email_data_file
        self.user = None
        self.password = None

    def load_credentials(self):
        with open(self.credentials_file_path) as f:
            content = f.read()
        my_credentials = yaml.load(content, Loader=yaml.FullLoader)
        self.user = my_credentials["user"]
        self.password = my_credentials["password"]

    def send_emails(self):
        # Load filtered email data
        filtered_email_data = pd.read_excel(self.filtered_email_data_file)
        matched_email_data = pd.read_excel(self.matched_email_data_file)
        original_email_data = pd.read_excel(self.original_email_data_file)

        # Load sent email data
        try:
            sent_email_data = pd.read_excel(self.sent_email_data_file)
        except FileNotFoundError:
            sent_email_data = pd.DataFrame(
                columns=['To', 'Subject', 'Body', 'Sent', 'Date received', 'Sent time'])

        # Iterate over matched email data and send emails
        new_emails = []
        for index, row in matched_email_data.iterrows():
            to_email = row['To_email']
            subject = row['Subject']
            body = row['Body']
            from_data = row['From']
            date_received = row['Date']

            # Check if the email with the same subject and date has been sent
            if ((sent_email_data['Subject'] == subject) & (
                    sent_email_data['Date received'] == date_received)).any():
                continue  # Skip previously sent emails

            # Construct email message
            message = MIMEMultipart()
            message['From'] = self.user
            message['To'] = to_email
            message['Subject'] = subject

            # Construct email body
            body = f'{from_data}\n\n{body}'
            body = re.sub(r'_x000D_', '', body)
            message.attach(MIMEText(body, 'plain'))

            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.user, self.password)
                server.send_message(message)

            # Record sent email
            sent_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_email = {'To': to_email, 'Subject': subject, 'Body': body, 'Sent': True,
                         'Date received': date_received, 'Sent time': sent_time}
            new_emails.append(new_email)

        # Concatenate new emails with existing sent email data
        sent_email_data = pd.concat([sent_email_data, pd.DataFrame(new_emails)], ignore_index=True)

        # Save sent email data to the Git repository
        repo = Repo.clone_from(self.repo_url, 'temp_repo')
        repo_path = repo.working_dir

        sent_email_data_file_path = f'{repo_path}/{self.sent_email_data_file}'
        sent_email_data.to_excel(sent_email_data_file_path, index=False)

        # Commit and push changes
        repo.git.add(sent_email_data_file_path)
        repo.index.commit("Update sent email data")
        origin = repo.remote(name='origin')
        origin.push()

    def run(self):
        self.load_credentials()
        self.send_emails()


class GmailDataExtractor:
    def __init__(self, credentials_file_path, mail_send_list_file_path, repo_url):
        self.credentials_file_path = credentials_file_path
        self.mail_send_list_file_path = mail_send_list_file_path
        self.repo_url = repo_url

    def load_credentials(self):
        with open(self.credentials_file_path) as f:
            content = f.read()
        my_credentials = yaml.load(content, Loader=yaml.FullLoader)
        self.user = my_credentials["user"]
        self.password = my_credentials["password"]

    def connect_to_gmail(self):
        imap_url = 'imap.gmail.com'
        self.mail = imaplib.IMAP4_SSL(imap_url)
        self.mail.login(self.user, self.password)

    def fetch_emails(self):
        self.mail.select('Inbox')
        seven_days_ago = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%d-%b-%Y")
        search_criteria = f'(SINCE {seven_days_ago}) TO "SupplyAdOps@media.net"'
        _, data = self.mail.search(None, search_criteria)
        mail_ids = data[0].split()

        df = pd.DataFrame(columns=['Subject', 'From', 'Date', 'Body'])

        for mail_id in mail_ids:
            _, data = self.mail.fetch(mail_id, '(RFC822)')
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)

            subject = msg['Subject']
            sender = msg['From']
            date = msg['Date']
            body = ''

            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == 'text/plain':
                        body += part.get_payload(decode=True).decode('utf-8').replace('_x000D_', '') + "\n\n"
            else:
                body = msg.get_payload(decode=True).decode('utf-8').replace('_x000D_', '')

            df = pd.concat([df, pd.DataFrame({'Subject': [subject], 'From': [sender], 'Date': [date], 'Body': [body]})])

        self.original_df = df
        self.filtered_df = df[~df['Subject'].str.startswith('Re:')]

    def load_mail_send_list(self):
        self.mail_send_list = pd.read_csv(self.mail_send_list_file_path)

    def match_emails(self):
        matched_df = pd.DataFrame(columns=['Subject', 'From', 'Date', 'Body', 'To_email'])

        for _, row in self.filtered_df.iterrows():
            subject = row['Subject']
            to_email = None

            for _, second_row in self.mail_send_list.iterrows():
                mail_start = second_row['Mail Start']
                if subject.startswith(mail_start):
                    to_email = second_row['Email']
                    break

            if to_email:
                matched_df = pd.concat(
                    [matched_df, pd.DataFrame({'Subject': [row['Subject']], 'From': [row['From']],
                                               'Date': [row['Date']], 'Body': [row['Body']],
                                               'To_email': [to_email]})])
            else:
                matched_df = pd.concat(
                    [matched_df, pd.DataFrame({'Subject': [row['Subject']], 'From': [row['From']],
                                               'Date': [row['Date']], 'Body': [row['Body']],
                                               'To_email': [
                                                   'a.t.900201378736.u-49287517.6295eb28-094a-4d6c-98dc-d6a3d60b85b7@tasks.clickup.com']})])

        self.matched_df = matched_df

    def store_data_to_repo(self):
        repo = Repo.clone_from(self.repo_url, 'temp_repo')
        repo_path = repo.working_dir

        # Save filtered email data
        filtered_email_data_file_path = f'{repo_path}/{self.filtered_email_data_file}'
        self.filtered_df.to_excel(filtered_email_data_file_path, index=False)
        repo.git.add(filtered_email_data_file_path)

        # Save matched email data
        matched_email_data_file_path = f'{repo_path}/{self.matched_email_data_file}'
        self.matched_df.rename(columns={'Email': 'To_email'}, inplace=True)
        self.matched_df.to_excel(matched_email_data_file_path, index=False)
        repo.git.add(matched_email_data_file_path)

        # Save original email data
        original_email_data_file_path = f'{repo_path}/{self.original_email_data_file}'
        self.original_df.to_excel(original_email_data_file_path, index=False)
        repo.git.add(original_email_data_file_path)

        # Commit and push changes
        repo.index.commit("Update email data")
        origin = repo.remote(name='origin')
        origin.push()

    def disconnect_from_gmail(self):
        self.mail.logout()

    def run(self):
        self.load_credentials()
        self.connect_to_gmail()
        self.fetch_emails()
        self.load_mail_send_list()
        self.match_emails()
        self.store_data_to_repo()
        self.disconnect_from_gmail()


# Usage
if __name__ == "__main__":
    credentials_file_path = "credentials.yml"  # Path to credentials file in the repository
    mail_send_list_file_path = "Mail_Send_List.csv"  # Path to mail send list file in the repository
    repo_url = "https://github.com/username/repo.git"  # Replace with the URL of your Git repository
    filtered_email_data_file = "filtered_email_data.xlsx"
    matched_email_data_file = "matched_email_data.xlsx"
    original_email_data_file = "original_email_data.xlsx"
    sent_email_data_file = "Sent_mail.xlsx"

    extractor = GmailDataExtractor(credentials_file_path, mail_send_list_file_path, repo_url)
    extractor.run()

    mailer = GmailMailer(credentials_file_path, mail_send_list_file_path, repo_url, filtered_email_data_file,
                         matched_email_data_file, original_email_data_file, sent_email_data_file)
    mailer.load_credentials()
    mailer.send_emails()
