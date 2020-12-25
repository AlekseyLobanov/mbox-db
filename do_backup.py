import sys
import mailbox
import hashlib
import json
import time
import os
import logging
import sqlite3
import itertools
import argparse
import base64
import binascii

import magic

import tqdm
import mailparser

logging.getLogger("mailparser").setLevel(logging.INFO)


class SimpleStorage:
    def __init__(self, base_path):
        self.base_path = base_path
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

    def is_exists(self, object_id):
        return os.path.exists(os.path.join(
            self.base_path,
            object_id[:2],
            object_id[2:]
        ))

    def save(self, object_id, data):
        base_dir = os.path.join(self.base_path, object_id[:2])
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)
        result_path = os.path.join(base_dir, object_id[2:])
        with open(result_path, "wb") as f:
            f.write(data)
        return result_path


class SqliteMetadataBackend:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `emails` (
              `id` varchar(64) NOT NULL,
              `subject` varchar(250)  NOT NULL default '',
              `dt` INTEGER,
               PRIMARY KEY  (`id`)
            );
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `attachments` (
              `id` varchar(64) NOT NULL,
              `source_name` varchar(250)  NOT NULL default '',
              `mime` varchar(40)  NOT NULL default '',
              `size` INTEGER NOT NULL,
               PRIMARY KEY  (`id`)
            );
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `contacts` (
              `id` INTEGER PRIMARY KEY,
              `email` varchar(128) NOT NULL UNIQUE,
              `name` varchar(250)
            );
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `contact_to_mail` (
              `mail_id` varchar(64) NOT NULL,
              `contact_id` INTEGER NOT NULL,
              `is_sender` BOOL NOT NULL,
               FOREIGN KEY (mail_id) REFERENCES emails(id),
               FOREIGN KEY (contact_id) REFERENCES contacts(id),
               UNIQUE (mail_id, contact_id, is_sender)
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `mail_to_attachment` (
              `mail_id` varchar(64) NOT NULL,
              `attachment_id` varchar(64) NOT NULL,
               FOREIGN KEY (mail_id) REFERENCES emails(id),
               FOREIGN KEY (attachment_id) REFERENCES attachments(id),
               UNIQUE (mail_id, attachment_id)
            );
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS `index_emails` ON `contacts`(`email`);
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS `index_dates` ON `emails`(`dt`);
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS `index_attachment_type` ON `attachments`(`mime`);
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS `index_attachment_size` ON `attachments`(`size`);
        """)


    def add_email(self, mail_id, from_list, to_list, mail_subject, mail_date):
        if isinstance(mail_subject, list):
            if mail_subject:
                mail_subject = mail_subject[0]
            else:
                mail_subject = ""
        try:
            return self._add_email(mail_id, from_list, to_list, mail_subject, mail_date)
        except sqlite3.InterfaceError:
            logging.exception(
                f"Unable to add email id {mail_id} from {from_list} to "
                f"{to_list} subject {mail_subject} date {mail_date}"
            )
        return False

    def _add_email(self, mail_id, from_list, to_list, mail_subject, mail_date):
        is_added = True
        contact_ids = {}
        for name, email in list(from_list) + list(to_list):
            try:
                self.cursor.execute(
                    "INSERT INTO `contacts` (`name`, `email`) VALUES (?, ?)",
                    [name, email]
                )
            except sqlite3.IntegrityError:
                logging.debug(f"Duplicate contact: {name} email: {email}")
            self.cursor.execute(
                "SELECT `id` FROM `contacts` WHERE email=?",
                [email]
            )
            contact_ids[email] = self.cursor.fetchall()[0][0]

        for _, email, is_sender in itertools.chain(
                map(lambda x: list(x) + [True], from_list),
                map(lambda x: list(x) + [False], to_list),
        ):
            try:
                self.cursor.execute(
                    "INSERT INTO `contact_to_mail` (`mail_id`, `contact_id`, is_sender) VALUES (?, ?, ?)",
                    [mail_id, contact_ids[email], is_sender]
                )
            except sqlite3.IntegrityError:
                logging.debug(
                    f"Duplicate contact_to_mail: {mail_id} to {contact_ids[email]} and {is_sender}")

        try:
            self.cursor.execute(
                f"""INSERT INTO `emails` (`id`, `subject`, `dt`)
                VALUES (?, ?, ?)""",
                [mail_id, mail_subject, mail_date]
            )
        except sqlite3.IntegrityError:
            is_added = False
            logging.debug(f"Duplicate mail: {mail_id}")
        self.conn.commit()
        return is_added

    def add_attachment(self, mail_id, attachment_id,
                       source_name, size, mime_type=""):
        is_added = True
        try:
            self.cursor.execute(
                f"""INSERT INTO `attachments` (`id`,`source_name`, `size`, `mime`)
                VALUES (?, ?, ?, ?)""",
                [attachment_id, source_name[:200], size, mime_type]
            )
        except sqlite3.IntegrityError:
            is_added = False
            logging.debug(f"Duplicate attachment_id: {attachment_id}")

        try:
            self.cursor.execute(
                f"""INSERT INTO `mail_to_attachment` (`mail_id`, `attachment_id`)
                VALUES (?, ?)""",
                [mail_id, attachment_id]
            )
        except sqlite3.IntegrityError:
            logging.debug(
                f"Duplicate relation: mail {mail_id} to attachment_id: {attachment_id}")
        self.conn.commit()
        return is_added


def process_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input", required=True, help="input .mbox file or folder to search recursively *.mbox files"
    )
    parser.add_argument(
        "-m", "--metadata", required=True, default="metadata.db", help="metadata DB path"
    )
    parser.add_argument(
        "-s", "--storage", required=True, help="mail objects root"
    )
    parser.add_argument(
        "-e", "--errors", help="directory to save all problem email"
    )
    parser.add_argument(
        "-p", "--progress", action="store_true", help="show progress"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="increase output verbosity"
    )
    return parser.parse_args()


def main():
    args = process_argparse()
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.DEBUG if args.verbose else logging.WARNING
    )
    metadata_backend = SqliteMetadataBackend(args.metadata)
    storage = SimpleStorage(args.storage)
    box = mailbox.mbox(args.input)
    errors_path = args.errors  # not folder, but path with prefix
    if errors_path:
        if not os.path.exists(errors_path):
            os.makedirs(errors_path)
        errors_path = os.path.join(
            errors_path,
            hashlib.sha256(args.input.encode("utf-8")).hexdigest()[:6] + "_",
        )

    new_emails_count = 0
    new_attachments_count = 0
    spam_count = 0
    errors_count = 0

    mail_generator = enumerate(tqdm.tqdm(box) if args.progress else box)
    for i, msg in mail_generator:
        if msg.get("X-Yandex-Spam") == "1":
            spam_count += 1
            continue
        try:
            mail = mailparser.parse_from_string(str(msg))
        except BaseException:
            if errors_path:
                with open(errors_path + f"{errors_count+1}.mbox", "wb") as f:
                    f.write(bytes(msg))
            errors_count += 1
            continue
        string_to_hash = str(mail.date.timestamp()) + \
            "".join(sorted(map(lambda x: x[1], mail.from_))) + \
            "".join(sorted(map(lambda x: x[1], mail.to))) + \
            str(mail.subject)
        hash_name = hashlib.sha256(string_to_hash.encode("utf-8")).hexdigest()
        if not storage.is_exists(hash_name):
            storage.save(hash_name, bytes(msg))
        if metadata_backend.add_email(
                mail_id=hash_name,
                from_list=mail.from_,
                to_list=mail.to,
                mail_subject=mail.subject,
                mail_date=mail.date.timestamp(),
        ):
            new_emails_count += 1

        payload = msg.get_payload()
        payloads_to_remove = []

        for part in payload:
            if isinstance(part, str):
                continue
            content_disposition = part.get_content_disposition()
            if content_disposition and content_disposition.startswith(
                    'attachment'):
                payloads_to_remove.append(part)
        for part in payloads_to_remove:
            payload.remove(part)
        for attachment in mail.attachments:
            try:
                if attachment["binary"]:
                    part_data = base64.b64decode(attachment["payload"])
                else:
                    part_data = attachment["payload"].encode("utf-8")
            except (binascii.Error, ValueError):
                logging.warning(f"Skipping attachment on mail {hash_name} because error")
            filename = hashlib.sha256(part_data).hexdigest()
            if not attachment["binary"]:
                logging.info(
                    f"text attachment {hash_name}: {filename} ({attachment['filename']})"
                )
            if not storage.is_exists(filename):
                out_path = storage.save(filename, part_data)
            if metadata_backend.add_attachment(
                    mail_id=hash_name,
                    attachment_id=filename,
                    source_name=attachment["filename"],
                    size=len(part_data),
                    mime_type=magic.from_buffer(part_data[:4096], mime=True)
            ):
                new_attachments_count += 1
    print(
        f"new mails: {new_emails_count}, "
        f"new attachments: {new_attachments_count}, "
        f"spam: {spam_count}, "
        f"errors: {errors_count}"
    )


if __name__ == "__main__":
    main()
