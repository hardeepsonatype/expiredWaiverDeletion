# expiredWaiverDeletion
Delete Expired Waivers in IQ Server

# Edit Configuration in repositoryWaivers.py :
IQ_SERVER_URL - Replace with your IQ Server URL

IQ_USERNAME - Replace with your username

IQ_PASSWORD - Replace with your password

# Run
1. Export Waivers from the Dashboard and place your "results-waivers-*.csv" file into the same directory as expiredWaiverDeletion.py
2. python expiredWaiverDeletion.py
3. Script will find waivers to delete, provide a list then prompt you to enter "DELETE" if you wish to continue and delete the waivers



