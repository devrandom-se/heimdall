# Migrating to a new database instance

RDS storage can never be decreased, and encryption at rest can only be set when an instance is
created. Stacks created before the template gained `StorageEncrypted` and `MaxAllocatedStorage`, or
instances that have autoscaled far beyond what they hold, are therefore moved to a **new** instance
with a logical copy (`pg_dump | psql`). The template runs both instances side by side while the
data is copied, so every step is an ordinary stack update and the old instance is never written to.

Time: ~15 minutes of stack updates spread over three days, plus one copy window of 1–3 hours.
Cost impact: storage of the new instance from step 2 until the old one is deleted in step 12.

## Parameters involved

| Parameter | Purpose |
|---|---|
| `DBInstanceSuffix` | Identifier of the new instance is `<stack>-db<suffix>`; the old one is `<stack>-db`. Use `2`. |
| `LegacyDatabase` | `keep` keeps the old instance in the stack; `none` deletes it with a final snapshot. |
| `ActiveDatabase` | `legacy` or `new`: which instance the tasks, IAM policies and outputs point at. |
| `DBMaxAllocatedStorage` | Autoscaling cap of the new instance (GB). |
| `DBAllocatedStorage` | Size of the new instance. Do **not** change it during the migration: it also feeds the old instance, and a decrease is impossible. |
| `BastionInstanceType` | `t4g.medium` on copy day (bandwidth), `t4g.nano` otherwise. |
| `EnableScheduledBackup` | `false` on copy day so no job writes to the old instance. |

All updates below use the change-set routine: create with `--no-execute-changeset`, review with
`describe-change-set`, execute only when the listed changes are the expected ones. **If `DBInstance`
appears as anything other than the intended Add/Remove, or `BackupBucket` appears at all, stop.**

```bash
STACK=my-heimdall
deploy() {  # usage: deploy Key=Value ...
  aws cloudformation deploy --stack-name "$STACK" --template-file heimdall-stack.yml \
    --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset --no-execute-changeset \
    --parameter-overrides "$@"
}
review() { aws cloudformation describe-change-set --change-set-name "$1" \
    --query 'Changes[].ResourceChange.[Action,LogicalResourceId,Replacement]' --output table; }
run() { aws cloudformation execute-change-set --change-set-name "$1" &&
    aws cloudformation wait stack-update-complete --stack-name "$STACK" && echo UPDATED; }
```

## Day 0: create the new instance

1. Check the stack's stored parameters: `DBAllocatedStorage` must equal the size you want for the new
   instance (it cannot be changed here, see above) and `EnableBastion` must be `true`. Also confirm
   that the engine major version in the template can still create instances (the list shows at least
   one `available` version):
   ```bash
   aws cloudformation describe-stacks --stack-name $STACK --query 'Stacks[0].Parameters' --output table
   aws rds describe-db-engine-versions --engine postgres --engine-version 15 \
     --query 'DBEngineVersions[].[EngineVersion,Status]' --output text | tail -3
   ```
2. Update A:
   ```bash
   deploy DBInstanceSuffix=2 LegacyDatabase=keep ActiveDatabase=legacy DBMaxAllocatedStorage=150 BastionInstanceType=t4g.medium
   ```
   Expected: Add `Database`; Modify `BastionInstance` (instance type, Replacement False), the task
   definitions, the schedule rule, the IAM roles/users whose policies now list both instance ARNs, and
   the outputs. `DBInstance` must not appear.
3. Set the new instance's master password from the SSM parameter (the stack's `DBPassword` value may
   be the placeholder):
   ```bash
   aws rds modify-db-instance --db-instance-identifier $STACK-db2 --apply-immediately \
     --master-user-password "$(aws ssm get-parameter --name /$STACK/rds/password --with-decryption --query Parameter.Value --output text)"
   ```
4. Optional: `aws rds stop-db-instance --db-instance-identifier $STACK-db2` until copy day.

## Copy day (start early; everything before the scheduled job time)

5. Update B: `deploy EnableScheduledBackup=false` — expected: Remove `BackupScheduleRule` only.
6. Start both instances and the bastion, wait until `available` / `running`:
   ```bash
   aws rds start-db-instance --db-instance-identifier $STACK-db
   aws rds start-db-instance --db-instance-identifier $STACK-db2
   B=$(aws cloudformation describe-stacks --stack-name $STACK --query 'Stacks[0].Outputs[?OutputKey==`BastionInstanceId`].OutputValue' --output text)
   aws ec2 start-instances --instance-ids $B
   aws rds wait db-instance-available --db-instance-identifier $STACK-db
   aws rds wait db-instance-available --db-instance-identifier $STACK-db2
   OLD=$(aws rds describe-db-instances --db-instance-identifier $STACK-db  --query 'DBInstances[0].Endpoint.Address' --output text)
   NEW=$(aws rds describe-db-instances --db-instance-identifier $STACK-db2 --query 'DBInstances[0].Endpoint.Address' --output text)
   echo "$OLD $NEW"
   ```
7. Open a shell on the bastion and run the copy under `nohup` (the session may drop; the copy must not):
   ```bash
   aws ssm start-session --target $B
   # --- on the bastion ---
   curl -sO https://raw.githubusercontent.com/devrandom-se/heimdall/main/db-migrate.sh
   export PGPASSWORD=$(aws ssm get-parameter --name /<stack>/rds/password --with-decryption --query Parameter.Value --output text --region <region>)
   # if the bastion role cannot read the parameter:  read -rs PGPASSWORD; export PGPASSWORD
   nohup bash db-migrate.sh <old-endpoint> <new-endpoint> > ~/db-migrate.log 2>&1 &
   tail -f ~/db-migrate.log
   ```
   The script installs the PostgreSQL client if needed, refuses a non-empty target, streams the dump,
   compares row counts per table and the indexes on `objects`, runs `VACUUM ANALYZE`, and ends with
   `MIGRATION OK` or `MIGRATION FAILED`.
8. Do not continue unless the log ends with `MIGRATION OK`.

## Cutover (same day, before the scheduled job time)

9. Update C: `deploy ActiveDatabase=new EnableScheduledBackup=true BastionInstanceType=t4g.nano`
   Expected: Modify task definitions and outputs, Add `BackupScheduleRule`, Modify IAM policies and
   the bastion. No database resource.
10. Stop what the job no longer manages: `aws rds stop-db-instance --db-instance-identifier $STACK-db`
    and `aws ec2 stop-instances --instance-ids $B`. The new instance is started and stopped by the job.
11. Next morning, verify the night's run as usual (job summary `Objects failed: 0`, exit code 0, the
    new instance stopped afterwards, `FreeStorageSpace` of the new instance as expected). `db-tunnel.sh`
    follows the stack output and now tunnels to the new instance; update `~/.pgpass` hosts.

## Decommission (after one green night)

12. Update D: `deploy LegacyDatabase=none` — expected: Remove `DBInstance`. CloudFormation takes a
    final snapshot before deleting (DeletionPolicy Snapshot). Delete that snapshot after your grace
    period; until then it costs snapshot storage for the used data.

## Rollback

Before step 12 the old instance is intact and untouched. `deploy ActiveDatabase=legacy
EnableScheduledBackup=true` points everything back at it. The new instance can stay (stopped) and the
copy can be redone another day. After step 12 the final snapshot is the way back.

## Notes

- `DeletionProtection` is on for the new instance. Before ever deleting the stack:
  `aws rds modify-db-instance --db-instance-identifier <id> --no-deletion-protection`.
- A stack without a bastion (`EnableBastion=false`) needs one for copy day: add `EnableBastion=true`
  to update A and `EnableBastion=false` to update C.
- The copy runs single-threaded; the index builds on `objects` dominate. A t4g.medium bastion moves
  data at ~30 MB/s sustained, which is why the nano is swapped out for the day.
