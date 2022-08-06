import win32com.client as client
import logging
import os
import re
import datetime
import pythoncom

logger = logging.getLogger("sending_service")

class ReadOutlookMail:

    _today_date = ''
    _mail_address = ''
    _parent_path = ''
    _read_outlook_response = False
    _message_subject = ''
    _attachment_present = False
    _file_name = ''
    _files_list = []

    def __init__(self, mail_id, parent_path):
        today_date = str(datetime.datetime.today()).split(" ")[0] + "_" + str(datetime.datetime.today()).split(" ")[1]
        self._today_date = today_date.replace(":", "_").replace(".", "_").replace("-", "_")
        self._mail_address = mail_id
        self._parent_path = parent_path
        self.get_read_mail()

    def get_read_mail(self):
        try:
            pythoncom.CoInitialize()
            outlook = ''
            outlook = client.Dispatch('Outlook.Application')
            namespace = outlook.GetNameSpace('MAPI')
            account = namespace.Folders[self._mail_address]
            inbox = account.Folders['Inbox']

            self._attachment_present = False
            self._files_list = []
            message_count = 1
            for message in inbox.Items:
                if message.UnRead == True:
                    self._message_subject = ''
                    self._message_subject = str(message.Subject)
                    if re.search('thermax', self._message_subject.lower()): # TODO: Need to add Pattern
                        sender_email_address = message.SenderEmailAddress
                        attachments = message.Attachments
                        if attachments:
                            self._attachment_present = True
                            num_attach = len([x for x in attachments])
                            path = os.path.join(self._parent_path, sender_email_address + "_" + self._today_date + "_" + str(message_count))
                            message_count = message_count + 1
                            if not os.path.exists(path):
                                os.mkdir(path)
                                for x in range(1, num_attach + 1):
                                    attachment = attachments.Item(x)
                                    attachment_type = str(attachment).split(".")[-1]
                                    if attachment_type.lower() in ["xls", "xlsx"]:
                                        file = path + "/" + str(attachment)
                                        file = file.strip()
                                        file = file.replace(" ", "_").replace("-", "_").replace("'", "").replace("#", "_No_").replace("&", "_").replace("(", "_").replace(")", "_")
                                        attachment.SaveASFile(file)
                                        self._file_name = file
                                        self._files_list.append({
                                            "file_name": self._file_name,
                                            "subject": self._message_subject
                                        })
                    message.Unread = False
            self._read_outlook_response = True
        except Exception:
            logger.error("Error in Get Read Mail Function in ReadOutlookMail Class!!!", exc_info=True)
            self._read_outlook_response = False

    def get_outlook_read_response(self):
        return self._read_outlook_response

    def get_files_list(self):
        return self._files_list


class SendOutlookMail:
    _receiver_email_address = ''
    _mail_subject = ''
    _html_body = ''
    _mail_body = ''
    _cc_email_address = ''
    _send_outlook_response = False

    def __init__(self, receiver_email_address, mail_subject, html_body, mail_body, cc_email_address):
        self._receiver_email_address = receiver_email_address
        self._mail_subject = mail_subject
        self._html_body = html_body
        self._mail_body = mail_body
        self._cc_email_address = cc_email_address
        self.get_send_mail()

    def get_send_mail(self):
        try:
            pythoncom.CoInitialize()
            outlook = ''
            outlook = client.Dispatch('Outlook.Application')
            mail = outlook.createItem(0)
            mail.To = self._receiver_email_address
            mail.Subject = self._mail_subject
            mail.Body = self._mail_body
            # mail.Attachments.Add('c:\\sample.xlsx')
            # mail.Attachments.Add('c:\\sample2.xlsx')
            mail.cc = self._cc_email_address
            mail.Send()
            self._send_outlook_response = True

        except Exception:
            logger.error("Error in Get Send Mail Function in SendOutlookMail Class!!!", exc_info=True)
            self._send_outlook_response = False

    def get_outlook_send_response(self):
        return self._send_outlook_response