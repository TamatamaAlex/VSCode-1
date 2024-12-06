#Not suggested. Refer to my comment on https://dai-corporation.atlassian.net/browse/DSEC-52?atlOrigin=eyJpIjoiYWMzNmZhZmRkN2Q5NDRhMDg2NWQ3Y2FkMWRiZTEzNDIiLCJwIjoiaiJ9
#Make a manifest then restore the files, then use the following command instead of this code: aws s3 sync s3://sourcebucketname s3://targetbucketname --force-glacier-transfer
aws s3api list-objects-v2 --bucket bucketname --query 'Contents[?StorageClass==`GLACIER`].{Key: Key}' --output text > glacier-objects.txt

while read -r key; do
    s3cmd restore s3://bucketname/"$key"
done < glacier-objects.txt