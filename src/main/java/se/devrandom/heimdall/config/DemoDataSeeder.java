package se.devrandom.heimdall.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import se.devrandom.heimdall.storage.PostgresService;

import java.sql.*;

@Component
@Profile("demo")
@DependsOn({"postgresService", "restoreService"})
public class DemoDataSeeder {

    private static final Logger log = LoggerFactory.getLogger(DemoDataSeeder.class);

    @Autowired
    private PostgresService postgresService;

    @Value("${postgres.backup.url}")
    private String dbUrl;

    @Value("${postgres.backup.username}")
    private String dbUsername;

    @Value("${postgres.backup.password}")
    private String dbPassword;

    @Value("${salesforce.org-id}")
    private String orgId;

    @PostConstruct
    public void seed() throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            // Check if data already exists
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM objects WHERE org_id = ?")) {
                ps.setString(1, orgId);
                ResultSet rs = ps.executeQuery();
                if (rs.next() && rs.getInt(1) > 0) {
                    log.info("Demo data already exists — skipping seed");
                    return;
                }
            }

            log.info("Seeding demo data...");
            seedCsvFiles(conn);
            seedObjects(conn);
            seedObjectStats(conn);
            seedRestoreLog(conn);
            log.info("Demo data seeded successfully");
        }
    }

    private void seedCsvFiles(Connection conn) throws SQLException {
        String sql = "INSERT INTO csvfiles (period, s3path) VALUES (?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // Period 2601 (Jan 2026) and 2602 (Feb 2026)
            for (int period : new int[]{2601, 2602}) {
                for (String obj : new String[]{"Account", "Contact", "Case", "EmailMessage", "ContentVersion"}) {
                    ps.setInt(1, period);
                    ps.setString(2, "s3://heimdall-demo/OrgId=" + orgId + "/Period=" + period + "/" + obj + "/data.csv");
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
    }

    private void seedObjects(Connection conn) throws SQLException {
        String sql = """
            INSERT INTO objects (id, period, version, metadata, org_id, csv_id, is_deleted, object_name, name)
            VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {

            // === ACCOUNTS ===

            // Acme Corporation — version 1 (Jan 2026)
            addRecord(ps, "001xx000003DGbA", 2601, 1706745600,
                    """
                    {"Id":"001xx000003DGbA","Name":"Acme Corporation","Industry":"Technology","Phone":"(415) 555-1234",\
                    "BillingStreet":"123 Market Street","BillingCity":"San Francisco","BillingState":"CA","BillingPostalCode":"94105",\
                    "BillingCountry":"United States","Website":"https://acme.example.com","NumberOfEmployees":"250",\
                    "AnnualRevenue":"45000000","OwnerId":"005xx000001Svqw","Type":"Customer - Direct",\
                    "Description":"Enterprise technology solutions provider.\\nFounded in 2010.\\nSpecializing in cloud infrastructure.",\
                    "CreatedDate":"2023-03-15T10:30:00.000Z","LastModifiedDate":"2026-01-15T14:22:00.000Z"}""",
                    1, false, "Account", "Acme Corporation");

            // Acme Corporation — version 2 (Feb 2026) — phone and employees changed
            addRecord(ps, "001xx000003DGbA", 2602, 1709337600,
                    """
                    {"Id":"001xx000003DGbA","Name":"Acme Corporation","Industry":"Technology","Phone":"(415) 555-9876",\
                    "BillingStreet":"123 Market Street","BillingCity":"San Francisco","BillingState":"CA","BillingPostalCode":"94105",\
                    "BillingCountry":"United States","Website":"https://acme.example.com","NumberOfEmployees":"275",\
                    "AnnualRevenue":"52000000","OwnerId":"005xx000001Svqw","Type":"Customer - Direct",\
                    "Description":"Enterprise technology solutions provider.\\nFounded in 2010.\\nSpecializing in cloud infrastructure.",\
                    "CreatedDate":"2023-03-15T10:30:00.000Z","LastModifiedDate":"2026-02-10T09:15:00.000Z"}""",
                    6, false, "Account", "Acme Corporation");

            // Globex Industries — deleted
            addRecord(ps, "001xx000003DGbB", 2602, 1709424000,
                    """
                    {"Id":"001xx000003DGbB","Name":"Globex Industries","Industry":"Manufacturing","Phone":"(312) 555-4567",\
                    "BillingStreet":"456 Industrial Blvd","BillingCity":"Chicago","BillingState":"IL","BillingPostalCode":"60601",\
                    "BillingCountry":"United States","Website":"https://globex.example.com","NumberOfEmployees":"1200",\
                    "AnnualRevenue":"180000000","OwnerId":"005xx000001Svqx","Type":"Customer - Channel",\
                    "Description":"Global manufacturing and distribution.","CreatedDate":"2022-08-20T16:00:00.000Z",\
                    "LastModifiedDate":"2026-02-01T11:45:00.000Z"}""",
                    6, true, "Account", "Globex Industries");

            // Initech Solutions — 2 versions
            addRecord(ps, "001xx000003DGbC", 2601, 1706832000,
                    """
                    {"Id":"001xx000003DGbC","Name":"Initech Solutions","Industry":"Consulting","Phone":"(512) 555-7890",\
                    "BillingStreet":"789 Congress Ave","BillingCity":"Austin","BillingState":"TX","BillingPostalCode":"78701",\
                    "BillingCountry":"United States","NumberOfEmployees":"85","AnnualRevenue":"12000000",\
                    "OwnerId":"005xx000001Svqw","Type":"Prospect",\
                    "Description":"IT consulting and staffing.","CreatedDate":"2024-01-10T08:00:00.000Z",\
                    "LastModifiedDate":"2026-01-20T13:30:00.000Z"}""",
                    1, false, "Account", "Initech Solutions");

            addRecord(ps, "001xx000003DGbC", 2602, 1709510400,
                    """
                    {"Id":"001xx000003DGbC","Name":"Initech Solutions","Industry":"Consulting","Phone":"(512) 555-7890",\
                    "BillingStreet":"789 Congress Ave","BillingCity":"Austin","BillingState":"TX","BillingPostalCode":"78701",\
                    "BillingCountry":"United States","NumberOfEmployees":"92","AnnualRevenue":"14500000",\
                    "OwnerId":"005xx000001Svqw","Type":"Customer - Direct",\
                    "Description":"IT consulting and staffing.\\nRecently expanded to managed services.",\
                    "CreatedDate":"2024-01-10T08:00:00.000Z","LastModifiedDate":"2026-02-12T10:00:00.000Z"}""",
                    6, false, "Account", "Initech Solutions");

            // Wayne Enterprises — archived
            addRecord(ps, "001xx000003DGbD", -2501, 1701388800,
                    """
                    {"Id":"001xx000003DGbD","Name":"Wayne Enterprises","Industry":"Financial Services","Phone":"(212) 555-3456",\
                    "BillingCity":"New York","BillingState":"NY","BillingCountry":"United States",\
                    "NumberOfEmployees":"5000","AnnualRevenue":"890000000","OwnerId":"005xx000001Svqx",\
                    "Type":"Customer - Direct","Description":"Diversified conglomerate.",\
                    "CreatedDate":"2021-06-01T12:00:00.000Z","LastModifiedDate":"2025-01-15T08:00:00.000Z"}""",
                    null, false, "Account", "Wayne Enterprises");

            // === CONTACTS ===

            // John Smith at Acme — 2 versions
            addRecord(ps, "003xx000004TGcA", 2601, 1706745600,
                    """
                    {"Id":"003xx000004TGcA","FirstName":"John","LastName":"Smith","Name":"John Smith",\
                    "Email":"john.smith@acme.example.com","Phone":"(415) 555-1235","Title":"VP of Engineering",\
                    "AccountId":"001xx000003DGbA","Department":"Engineering","MailingCity":"San Francisco",\
                    "MailingState":"CA","OwnerId":"005xx000001Svqw",\
                    "CreatedDate":"2023-04-01T09:00:00.000Z","LastModifiedDate":"2026-01-15T14:30:00.000Z"}""",
                    1, false, "Contact", "John Smith");

            addRecord(ps, "003xx000004TGcA", 2602, 1709337600,
                    """
                    {"Id":"003xx000004TGcA","FirstName":"John","LastName":"Smith","Name":"John Smith",\
                    "Email":"john.smith@acme.example.com","Phone":"(415) 555-1235","Title":"CTO",\
                    "AccountId":"001xx000003DGbA","Department":"Engineering","MailingCity":"San Francisco",\
                    "MailingState":"CA","OwnerId":"005xx000001Svqw",\
                    "CreatedDate":"2023-04-01T09:00:00.000Z","LastModifiedDate":"2026-02-05T16:00:00.000Z"}""",
                    6, false, "Contact", "John Smith");

            // Jane Doe at Globex — deleted (with parent account)
            addRecord(ps, "003xx000004TGcB", 2602, 1709424000,
                    """
                    {"Id":"003xx000004TGcB","FirstName":"Jane","LastName":"Doe","Name":"Jane Doe",\
                    "Email":"jane.doe@globex.example.com","Phone":"(312) 555-4568","Title":"Director of Operations",\
                    "AccountId":"001xx000003DGbB","Department":"Operations","MailingCity":"Chicago",\
                    "MailingState":"IL","OwnerId":"005xx000001Svqx",\
                    "CreatedDate":"2022-09-15T11:00:00.000Z","LastModifiedDate":"2026-02-01T11:50:00.000Z"}""",
                    6, true, "Contact", "Jane Doe");

            // Bob Johnson at Initech
            addRecord(ps, "003xx000004TGcC", 2602, 1709510400,
                    """
                    {"Id":"003xx000004TGcC","FirstName":"Bob","LastName":"Johnson","Name":"Bob Johnson",\
                    "Email":"bob.j@initech.example.com","Phone":"(512) 555-7891","Title":"Project Manager",\
                    "AccountId":"001xx000003DGbC","Department":"PMO","MailingCity":"Austin",\
                    "MailingState":"TX","OwnerId":"005xx000001Svqw",\
                    "CreatedDate":"2024-02-20T14:00:00.000Z","LastModifiedDate":"2026-02-12T10:05:00.000Z"}""",
                    6, false, "Contact", "Bob Johnson");

            // === CASES ===

            // Case 1 — "Login page not loading" — 2 versions (status changed)
            addRecord(ps, "500xx000005RHdA", 2601, 1706832000,
                    """
                    {"Id":"500xx000005RHdA","CaseNumber":"00001042","Subject":"Login page not loading after update",\
                    "Status":"New","Priority":"High","Origin":"Web","ContactId":"003xx000004TGcA",\
                    "AccountId":"001xx000003DGbA","OwnerId":"005xx000001Svqw","Type":"Problem",\
                    "Description":"After the latest platform update, users are unable to access the login page.\\n\\nSteps to reproduce:\\n1. Navigate to https://app.acme.example.com\\n2. Page shows a blank white screen\\n3. Browser console shows 404 errors for static assets\\n\\nAffects approximately 250 users.\\nStarted occurring after the 2026-01-18 deployment.",\
                    "CreatedDate":"2026-01-20T08:15:00.000Z","LastModifiedDate":"2026-01-20T13:45:00.000Z"}""",
                    1, false, "Case", "Login page not loading after update");

            addRecord(ps, "500xx000005RHdA", 2602, 1709337600,
                    """
                    {"Id":"500xx000005RHdA","CaseNumber":"00001042","Subject":"Login page not loading after update",\
                    "Status":"Closed","Priority":"High","Origin":"Web","ContactId":"003xx000004TGcA",\
                    "AccountId":"001xx000003DGbA","OwnerId":"005xx000001Svqw","Type":"Problem",\
                    "Description":"After the latest platform update, users are unable to access the login page.\\n\\nSteps to reproduce:\\n1. Navigate to https://app.acme.example.com\\n2. Page shows a blank white screen\\n3. Browser console shows 404 errors for static assets\\n\\nAffects approximately 250 users.\\nStarted occurring after the 2026-01-18 deployment.\\n\\nResolution: CDN cache was stale. Purged and verified.",\
                    "CreatedDate":"2026-01-20T08:15:00.000Z","LastModifiedDate":"2026-02-03T15:20:00.000Z"}""",
                    6, false, "Case", "Login page not loading after update");

            // Case 2 — "Cannot export reports"
            addRecord(ps, "500xx000005RHdB", 2602, 1709510400,
                    """
                    {"Id":"500xx000005RHdB","CaseNumber":"00001087","Subject":"Cannot export reports to CSV",\
                    "Status":"In Progress","Priority":"Medium","Origin":"Email","ContactId":"003xx000004TGcC",\
                    "AccountId":"001xx000003DGbC","OwnerId":"005xx000001Svqw","Type":"Problem",\
                    "Description":"When clicking Export on any report, the download starts but the file is empty (0 bytes).\\n\\nBrowser: Chrome 121\\nOS: Windows 11\\n\\nWorkaround: Export to Excel format works correctly.",\
                    "CreatedDate":"2026-02-10T09:30:00.000Z","LastModifiedDate":"2026-02-12T10:10:00.000Z"}""",
                    6, false, "Case", "Cannot export reports to CSV");

            // === EMAIL MESSAGES ===

            // Email 1 — Welcome email with HTML body
            addRecord(ps, "02sxx000006UIeA", 2602, 1709337600,
                    """
                    {"Id":"02sxx000006UIeA","Subject":"Welcome to Acme — Getting Started Guide",\
                    "FromAddress":"onboarding@acme.example.com","ToAddress":"john.smith@acme.example.com",\
                    "FromName":"Acme Onboarding","Status":"0","MessageDate":"2026-01-15T10:00:00.000Z",\
                    "ParentId":"500xx000005RHdA","RelatedToId":"001xx000003DGbA",\
                    "HtmlBody":"<html><body style=\\"font-family: Arial, sans-serif; color: #333;\\"><div style=\\"max-width: 600px; margin: 0 auto;\\"><div style=\\"background: #0176d3; padding: 20px; text-align: center;\\"><h1 style=\\"color: white; margin: 0;\\">Welcome to Acme</h1></div><div style=\\"padding: 20px;\\"><p>Hi John,</p><p>We're excited to have you on board! Here's what you need to get started:</p><ol><li><strong>Set up your profile</strong> — Complete your user settings</li><li><strong>Connect your tools</strong> — Integrate with Slack, Jira, and GitHub</li><li><strong>Join the team workspace</strong> — Your team is waiting for you</li></ol><p>If you have any questions, reply to this email or visit our <a href=\\"#\\">Help Center</a>.</p><p style=\\"color: #706e6b; font-size: 12px; margin-top: 30px;\\">Best regards,<br>The Acme Team</p></div></div></body></html>",\
                    "TextBody":"Hi John, We're excited to have you on board!",\
                    "CreatedDate":"2026-01-15T10:00:00.000Z","LastModifiedDate":"2026-01-15T10:00:00.000Z"}""",
                    6, false, "EmailMessage", "Welcome to Acme — Getting Started Guide");

            // Email 2 — Meeting follow-up
            addRecord(ps, "02sxx000006UIeB", 2602, 1709424000,
                    """
                    {"Id":"02sxx000006UIeB","Subject":"Re: Q1 Planning Meeting — Action Items",\
                    "FromAddress":"jane.doe@globex.example.com","ToAddress":"sales@acme.example.com",\
                    "FromName":"Jane Doe","Status":"0","MessageDate":"2026-02-01T15:30:00.000Z",\
                    "ParentId":"500xx000005RHdB","RelatedToId":"001xx000003DGbB",\
                    "HtmlBody":"<html><body style=\\"font-family: Georgia, serif; color: #222;\\"><p>Hi team,</p><p>Thanks for the productive meeting today. Here are the action items we agreed on:</p><table style=\\"border-collapse: collapse; width: 100%; margin: 15px 0;\\"><tr style=\\"background: #f4f6f9;\\"><th style=\\"border: 1px solid #ddd; padding: 8px; text-align: left;\\">Action Item</th><th style=\\"border: 1px solid #ddd; padding: 8px; text-align: left;\\">Owner</th><th style=\\"border: 1px solid #ddd; padding: 8px; text-align: left;\\">Due Date</th></tr><tr><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Finalize Q1 budget</td><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Jane</td><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Feb 15</td></tr><tr><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Review vendor proposals</td><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Bob</td><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Feb 20</td></tr><tr><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Update project timeline</td><td style=\\"border: 1px solid #ddd; padding: 8px;\\">John</td><td style=\\"border: 1px solid #ddd; padding: 8px;\\">Feb 10</td></tr></table><p>Let me know if I missed anything.</p><p>Best,<br>Jane</p></body></html>",\
                    "TextBody":"Thanks for the productive meeting today. Here are the action items...",\
                    "CreatedDate":"2026-02-01T15:30:00.000Z","LastModifiedDate":"2026-02-01T15:30:00.000Z"}""",
                    6, false, "EmailMessage", "Re: Q1 Planning Meeting — Action Items");

            // === CONTENT VERSIONS ===

            // Q4 Report PDF
            addRecord(ps, "068xx000007VJfA", 2602, 1709337600,
                    """
                    {"Id":"068xx000007VJfA","Title":"Q4_2025_Financial_Report","PathOnClient":"Q4_2025_Financial_Report.pdf",\
                    "FileType":"PDF","FileExtension":"pdf","ContentSize":"2458624",\
                    "Checksum":"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6","VersionNumber":"1",\
                    "ContentDocumentId":"069xx000008WKgA","OwnerId":"005xx000001Svqw",\
                    "CreatedDate":"2026-01-10T09:00:00.000Z","LastModifiedDate":"2026-01-10T09:00:00.000Z"}""",
                    6, false, "ContentVersion", "Q4_2025_Financial_Report.pdf");

            // Company Logo PNG
            addRecord(ps, "068xx000007VJfB", 2602, 1709424000,
                    """
                    {"Id":"068xx000007VJfB","Title":"Acme_Logo_2026","PathOnClient":"Acme_Logo_2026.png",\
                    "FileType":"PNG","FileExtension":"png","ContentSize":"184320",\
                    "Checksum":"f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3","VersionNumber":"2",\
                    "ContentDocumentId":"069xx000008WKgB","OwnerId":"005xx000001Svqx",\
                    "CreatedDate":"2025-06-15T14:00:00.000Z","LastModifiedDate":"2026-02-01T12:00:00.000Z"}""",
                    6, false, "ContentVersion", "Acme_Logo_2026.png");

            ps.executeBatch();
            log.info("Seeded {} demo records", 18);
        }
    }

    private void addRecord(PreparedStatement ps, String id, int period, int version,
                           String metadata, Integer csvId, boolean isDeleted,
                           String objectName, String name) throws SQLException {
        ps.setString(1, id);
        ps.setInt(2, period);
        ps.setInt(3, version);
        ps.setString(4, metadata);
        ps.setString(5, orgId);
        if (csvId != null) {
            ps.setInt(6, csvId);
        } else {
            ps.setNull(6, Types.INTEGER);
        }
        ps.setBoolean(7, isDeleted);
        ps.setString(8, objectName);
        ps.setString(9, name);
        ps.addBatch();
    }

    private void seedObjectStats(Connection conn) throws SQLException {
        String sql = """
            INSERT INTO object_stats (org_id, object_name, unique_records, total_versions, deleted_count, archived_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            addStat(ps, "Account", 4, 6, 1, 1);
            addStat(ps, "Contact", 3, 4, 1, 0);
            addStat(ps, "Case", 2, 3, 0, 0);
            addStat(ps, "EmailMessage", 2, 2, 0, 0);
            addStat(ps, "ContentVersion", 2, 2, 0, 0);
            ps.executeBatch();
        }
    }

    private void addStat(PreparedStatement ps, String objectName,
                         int unique, int total, int deleted, int archived) throws SQLException {
        ps.setString(1, orgId);
        ps.setString(2, objectName);
        ps.setInt(3, unique);
        ps.setInt(4, total);
        ps.setInt(5, deleted);
        ps.setInt(6, archived);
        ps.addBatch();
    }

    private void seedRestoreLog(Connection conn) throws SQLException {
        String sql = """
            INSERT INTO restore_log (original_id, new_id, object_name, period, sandbox_name, restored_fields)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "003xx000004TGcA");
            ps.setString(2, "003xx000004TGcA");
            ps.setString(3, "Contact");
            ps.setInt(4, 2601);
            ps.setString(5, "devpro");
            ps.setString(6, "Title,Phone");
            ps.addBatch();

            ps.setString(1, "001xx000003DGbB");
            ps.setString(2, "001xx000003XYzN");
            ps.setString(3, "Account");
            ps.setInt(4, 2602);
            ps.setString(5, "devpro");
            ps.setString(6, null);
            ps.addBatch();

            ps.executeBatch();
        }
    }
}
