Tool for porting WordPress comments to Coral Talk.

This is written in Python because my patience with JavaScript was expiring. You need `MySQLdb`,
`pymongo` and `lxml`.

This can be used to extract Disqus comments, but you _must_ have had the WordPress Disqus plugin
option which copies Disqus comments into WordPress' database enabled: this doesn't work
retrospectively. You also don't get upvote records or many user account details.

There's a bit of a hack in here because WordPress comments don't have to be tied to a user account:
we construct fake users based on the supplied username and email and disambiguate the usernames by
appending variable quantities of zero-width space characters. (Look, I said it was a hack.) The
default minimum number appended is 1, so none of the usernames are occupied in the database, so
your existing users aren't prevented from registering on the new site. Alternatively you could
attempt to do something to reunite them with their accounts.
