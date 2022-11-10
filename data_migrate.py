import aw_datastore
from aw_datastore import Datastore
import iso8601
from aw_core.models import Event
import time
start_time = time.time()

mongodb = Datastore(aw_datastore.get_storage_methods()["mongodb"], testing=False)
postgres = Datastore(aw_datastore.get_storage_methods()["peewee"], testing=True)

mongo_users = mongodb.get_all_users()
postgres_users = postgres.get_all_users()
postgres_users_obj = {x['email']: x for x in postgres_users}

def filterUser(user):
  try:
    if postgres_users_obj[user['email']]:
      return False
    else:
      return True
  except:
    return True

migrate_users = filter(filterUser, mongo_users)
migrate_users_total = 0
migrate_users_success = 0
migrate_users_fail = 0


for user in migrate_users:
  migrate_users_total += 1
  try:
    migrate_users_success += 1
    postgres.save_user(user)
  except:
    migrate_users_fail += 1
print(f"Start migrate {migrate_users_total} users, {migrate_users_success} migrate success, {migrate_users_fail} migrate fail")

mongo_buckets = mongodb.buckets()
postgres_buckets = postgres.buckets()
def filterBuckets(bucket):
  try:
    if postgres_buckets[bucket]:
      return False
    else:
      return True
  except:
    return True
migrate_buckets = filter(filterBuckets, mongo_buckets)
# migrate_buckets = [list(migrate_buckets)[0]]

def mapEvent(event: Event):
  del event['id']
  return event

for bucket in migrate_buckets:
  try:
    print(f"Start migrate bucket {bucket}")
    migrate_users_total += 1
    meta_data = mongo_buckets[bucket]
    postgres.create_bucket(bucket, 
      type=meta_data['type'], 
      client=meta_data['client'], 
      hostname=meta_data['hostname'], 
      created=iso8601.parse_date(meta_data['created']), 
      name=meta_data['name']
    )
    bucket_events = mongodb.storage_strategy.get_events(bucket, -1)
    _bucket_events = list(map(mapEvent, bucket_events))
    print(f"Bucket {bucket} is migrating {len(_bucket_events)} events ...")
    postgres.storage_strategy.insert_many(bucket, _bucket_events)
    print(f"Bucket {bucket} migrate done! ")

  except Exception as e:
    print(e)

print("--- %s seconds ---" % (time.time() - start_time))