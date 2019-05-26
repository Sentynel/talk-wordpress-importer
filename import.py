#! /usr/bin/env python3
import argparse
import hashlib
import re

import lxml.html
import lxml.etree
import MySQLdb
import pymongo

parser = argparse.ArgumentParser()
parser.add_argument("baseurl", help="Base URL of WP install, e.g. https://www.angrymetalguy.com (no trailing slash")
parser.add_argument("sql_dbname", help="Name of WP SQL database")
parser.add_argument("tableprefix", help="WP table name prefix (no trailing underscore)")
parser.add_argument("mongo_dbname", help="Name of Mongo DB")
args = parser.parse_args()

sql = MySQLdb.connect(db=args.sql_dbname, charset="utf8")
cur = sql.cursor()

posts = {}
users = {}
user_wildcards = {}
user_names = {}
user_obf = {}
comments = {}
zwsp = "\u200b"

def generate_user_id(author, email):
    simple = author + "|" + email
    if simple in users or "*" not in email:
        return simple
    if simple in user_wildcards:
        return user_wildcards[simple]
    wildcard = re.escape(author + "|") + re.escape(email).replace("\\*", ".") + "$"
    check = re.compile(wildcard)
    for user in users:
        if check.match(user):
            user_wildcards[simple] = user
            return user
    # no matches
    user_wildcards[simple] = simple
    return simple

def generate_expanded_user(author, uid):
    exp = author.lower() + zwsp
    n = 1
    while exp in user_names:
        exp += zwsp
        n += 1
    user_names[exp] = uid
    return author + n * zwsp, exp

html_comment_re = re.compile("<--")
def html_parse(body):
    body = html_comment_re.sub("&lt;", body)
    e = lxml.html.fromstring(body)
    tidied = lxml.html.tostring(e, encoding="unicode").replace("\n", "<br>")
    stripped = e.text_content()
    return stripped, tidied

cur.execute("select comment_ID, comment_approved, comment_parent, comment_date_gmt, comment_content, comment_post_ID, comment_author, comment_author_email from {}_comments".format(args.tableprefix))
for i in cur:
    comment_ID, comment_approved, comment_parent, comment_date_gmt, comment_content, comment_post_ID, comment_author, comment_author_email = i
    if not comment_content.strip():
        #print("empty comment:", comment_ID)
        continue
    comment_ID = str(comment_ID)
    comment_post_ID = str(comment_post_ID)
    user_id = generate_user_id(comment_author, comment_author_email)
    if user_id in user_obf:
        obfuscated_user_id = user_obf[user_id]
    else:
        obfuscated_user_id = hashlib.md5(("dNSUJpxfvWmu" + user_id).encode("utf8")).hexdigest()
        user_obf[user_id] = obfuscated_user_id
    if user_id not in users:
        user_expanded_name, user_expanded_name_lower = generate_expanded_user(comment_author, user_id)
        users[user_id] = {
                "id": obfuscated_user_id,
                "username": user_expanded_name,
                "lowercaseUsername": user_expanded_name_lower,
                "profiles": [{"provider": "disqus", "id": obfuscated_user_id}],
                "metadata": {"source": "wpimport"},
                "created_at": comment_date_gmt, # assume this is the chronological first comment by this user
                }

    if comment_post_ID not in posts:
        cur.execute("select post_name, post_title, post_date_gmt from {}_posts where ID=%s".format(args.tableprefix), (int(comment_post_ID),))
        try:
            post_name, post_title, post_date_gmt = cur.fetchone()
        except TypeError:
            print("comment ID:", comment_ID, "missing post ID:", comment_post_ID)
            continue
        posts[comment_post_ID] = {
                "id": comment_post_ID,
                "url": "{}/{}/".format(args.baseurl, post_name),
                "title": post_title,
                "scraped": None,
                "metadata": {"source": "wpimport"},
                "created_at": post_date_gmt,
                "publication_date": post_date_gmt,
                }

    try:
        stripped, tidied = html_parse(comment_content)
    except (lxml.etree.XMLSyntaxError, lxml.etree.ParserError) as e:
        print("comment failed parse:", e, [comment_content])
        continue
    comments[comment_ID] = {
            "status": "ACCEPTED" if comment_approved == "1" else "REJECTED",
            "id": comment_ID,
            "author_id": obfuscated_user_id,
            "parent_id": str(comment_parent) if comment_parent else None,
            "created_at": comment_date_gmt,
            "updated_at": comment_date_gmt,
            "asset_id": comment_post_ID,
            "body": stripped,
            "metadata": {"richTextBody": tidied, "source": "wpimport"},
            "reply_count": 0,
            }
    if comment_parent and str(comment_parent) in comments:
        comments[str(comment_parent)]["reply_count"] += 1

print(len(posts), len(users), len(comments))

mdb = getattr(pymongo.MongoClient(), args.mongo_dbname)
try:
    mdb.users.insert_many(users.values())
    print("user import done")
    mdb.assets.insert_many(posts.values())
    print("asset import done")
    mdb.comments.insert_many(comments.values())
    print("comment import done")
except pymongo.errors.BulkWriteError as e:
    print("failed", e, e.details)
