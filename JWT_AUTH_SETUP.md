# JWT Authentication Setup for Heimdall

Heimdall now supports JWT authentication, matching the bash script's authentication method.

## Prerequisites

1. **Salesforce Connected App** with JWT enabled
2. **Private Key** (salesforce-backup.key)
3. **User** pre-authorized in the Connected App

## Configuration Steps

### 1. Update application.properties

Replace your current OAuth2 config with JWT config:

```properties
# BEFORE (OAuth2 Client Credentials)
salesforce.grant-type=client_credentials
salesforce.client-id=YOUR_CLIENT_ID
salesforce.client-secret=YOUR_CLIENT_SECRET

# AFTER (JWT Bearer Token)
salesforce.grant-type=urn:ietf:params:oauth:grant-type:jwt-bearer
salesforce.client-id=YOUR_CONNECTED_APP_CLIENT_ID
salesforce.username=your.user@yourcompany.com
salesforce.jwt-key-file=/path/to/salesforce-backup.key
salesforce.audience-url=https://login.salesforce.com  # or https://test.salesforce.com for sandbox
```

### 2. Place Your Private Key

Copy your JWT private key to the path specified in `salesforce.jwt-key-file`:

```bash
cp /path/from/bash-script/salesforce-backup.key /path/for/heimdall/salesforce-backup.key
```

### 3. Verify Key Format (PKCS#8)

Your key should be in PKCS#8 PEM format. If you have a PKCS#1 key, convert it:

```bash
# Convert PKCS#1 to PKCS#8
openssl pkcs8 -topk8 -inform PEM -outform PEM -in salesforce-backup.key -out salesforce-backup-pkcs8.key -nocrypt
```

Then update `salesforce.jwt-key-file` to point to the PKCS#8 key.

### 4. Audience URL

- **Production orgs**: `https://login.salesforce.com`
- **Sandbox orgs**: `https://test.salesforce.com`
- **Custom domains**: Use your My Domain URL

## Testing

Run Heimdall and check the logs:

```bash
mvn spring-boot:run
```

You should see:
```
INFO  s.d.h.salesforce.SalesforceService : Using JWT authentication
INFO  s.d.h.salesforce.SalesforceService : Successfully authenticated with JWT for user: your.user@yourcompany.com
```

## Troubleshooting

### Error: "JWT key file not found"
- Check the file path in `salesforce.jwt-key-file`
- Use absolute path, not relative path

### Error: "Invalid private key format"
- Convert your key to PKCS#8 format (see step 3 above)
- Ensure the file contains only the private key (no certificates)

### Error: "invalid_grant: user hasn't approved this consumer"
- Go to Salesforce Setup → Connected Apps → Manage Connected Apps
- Click your app → Edit Policies
- Under "Permitted Users" → "Admin approved users are pre-authorized"
- Add your user to a Permission Set assigned to the Connected App

### Error: "invalid_grant: IP restricted"
- Go to Connected App settings
- Under "IP Relaxation" → "Relax IP restrictions"

## Comparison with Bash Script

Heimdall JWT auth is **identical** to bash script's `sf login org jwt`:

| Aspect | Bash Script | Heimdall |
|--------|-------------|----------|
| Auth Method | JWT Bearer | JWT Bearer ✅ |
| Grant Type | urn:ietf:params:oauth:grant-type:jwt-bearer | urn:ietf:params:oauth:grant-type:jwt-bearer ✅ |
| Token Expiry | 5 minutes | 5 minutes ✅ |
| Signature Algorithm | RS256 | RS256 ✅ |
| Claims | iss, sub, aud, exp | iss, sub, aud, exp ✅ |

Both use the same Connected App and private key!
