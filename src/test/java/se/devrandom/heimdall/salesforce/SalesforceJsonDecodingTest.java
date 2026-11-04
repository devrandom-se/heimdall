package se.devrandom.heimdall.salesforce;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import se.devrandom.heimdall.config.SalesforceCredentials;
import se.devrandom.heimdall.config.WebClientConfig;
import se.devrandom.heimdall.salesforce.objects.BulkQueryRequest;
import se.devrandom.heimdall.salesforce.objects.BulkQueryResult;
import se.devrandom.heimdall.salesforce.objects.DescribeGlobalResult;
import se.devrandom.heimdall.salesforce.objects.DescribeSObjectResult;
import se.devrandom.heimdall.salesforce.objects.Field;
import se.devrandom.heimdall.salesforce.objects.Heimdall_Backup_Config__c;
import se.devrandom.heimdall.salesforce.requests.DescribeGlobal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Decodes Salesforce REST responses through the WebClient built by {@link WebClientConfig},
 * served over real HTTP, so the codecs used in production are the ones under test.
 * The payloads are shaped like Salesforce API v61.0 responses, including fields Heimdall
 * does not map (Salesforce adds new ones in most releases).
 */
class SalesforceJsonDecodingTest {

    private static final String API = "v61.0";

    private HttpServer server;
    private final Map<String, String> responses = new java.util.concurrent.ConcurrentHashMap<>();
    private WebClient webClient;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String body = responses.get(exchange.getRequestURI().getPath());
            byte[] bytes = (body != null ? body : "[{\"errorCode\":\"NOT_FOUND\"}]").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json;charset=UTF-8");
            exchange.sendResponseHeaders(body != null ? 200 : 404, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();

        SalesforceCredentials credentials = new SalesforceCredentials();
        credentials.setBaseUrl("http://127.0.0.1:" + server.getAddress().getPort());
        credentials.setApiVersion(API);
        webClient = new WebClientConfig().webClient(credentials);
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private <T> T get(String path, Class<T> type) {
        return webClient.get().uri(path).retrieve().bodyToMono(type).block();
    }

    @Test
    void oauthTokenResponseMapsSnakeCaseFields() {
        responses.put("/services/oauth2/token", """
                {"access_token":"00DXX0000000001!AQEAQH.token","signature":"c2lnbmF0dXJl",
                 "scope":"api","instance_url":"https://example.my.salesforce.com",
                 "api_instance_url":"https://api.salesforce.com",
                 "id":"https://login.salesforce.com/id/00DXX0000000001EAA/005XX000001Sv6AAE",
                 "token_type":"Bearer","issued_at":"1793714881000"}
                """);

        SalesforceAccessToken token = get("/services/oauth2/token", SalesforceAccessToken.class);

        assertEquals("00DXX0000000001!AQEAQH.token", token.accessToken);
        assertEquals("https://example.my.salesforce.com", token.instanceUrl);
        assertEquals("https://login.salesforce.com/id/00DXX0000000001EAA/005XX000001Sv6AAE", token.id);
        assertEquals("Bearer", token.tokenType);
        assertEquals("api", token.scope);
        assertEquals("1793714881000", token.issuedAt);
        assertEquals("c2lnbmF0dXJl", token.signature);
    }

    @Test
    void describeGlobalIgnoresUnmappedFields() {
        responses.put("/services/data/" + API + "/sobjects/", """
                {"encoding":"UTF-8","maxBatchSize":200,"sobjects":[
                  {"activateable":false,"associateEntityType":null,"associateParentEntity":null,
                   "createable":true,"custom":false,"customSetting":false,"deepCloneable":false,
                   "deletable":true,"deprecatedAndHidden":false,"feedEnabled":true,"hasSubtypes":false,
                   "isInterface":false,"isSubtype":false,"keyPrefix":"001","label":"Account",
                   "labelPlural":"Accounts","layoutable":true,"mergeable":true,"mruEnabled":true,
                   "name":"Account","queryable":true,"replicateable":true,"retrieveable":true,
                   "searchable":true,"triggerable":true,"undeletable":true,"updateable":true,
                   "urls":{"sobject":"/services/data/v61.0/sobjects/Account",
                           "describe":"/services/data/v61.0/sobjects/Account/describe",
                           "rowTemplate":"/services/data/v61.0/sobjects/Account/{ID}"}},
                  {"activateable":false,"createable":false,"custom":false,"customSetting":false,
                   "deletable":false,"deprecatedAndHidden":false,"feedEnabled":false,"keyPrefix":null,
                   "label":"Account History","labelPlural":"Account History","layoutable":false,
                   "mergeable":false,"mruEnabled":false,"name":"AccountHistory","queryable":true,
                   "replicateable":false,"retrieveable":true,"searchable":false,"triggerable":false,
                   "undeletable":false,"updateable":false,"urls":{}}]}
                """);

        DescribeGlobal describe = DescribeGlobal.request(token(), webClient, API);

        assertEquals("UTF-8", describe.encoding);
        assertEquals(200, describe.maxBatchSize);
        assertEquals(2, describe.sobjects.size());
        DescribeGlobalResult account = describe.sobjects.get(0);
        assertEquals("Account", account.name);
        assertEquals("001", account.keyPrefix);
        assertEquals(Boolean.TRUE, account.queryable);
        assertEquals(Boolean.TRUE, account.replicateable);
        assertEquals("/services/data/v61.0/sobjects/Account/describe", account.urls.get("describe"));
        DescribeGlobalResult history = describe.sobjects.get(1);
        assertNull(history.keyPrefix);
        assertEquals(Boolean.FALSE, history.replicateable);
        assertTrue(history.urls.isEmpty());
    }

    @Test
    void describeSObjectMapsFieldsPicklistsAndRelationships() {
        responses.put("/services/data/" + API + "/sobjects/Widget__c/describe", """
                {"name":"Widget__c","keyPrefix":"a05","replicateable":true,"queryable":true,
                 "urls":{"describe":"/services/data/v61.0/sobjects/Widget__c/describe"},
                 "childRelationships":[
                   {"cascadeDelete":true,"childSObject":"Widget_Part__c","deprecatedAndHidden":false,
                    "field":"Widget__c","junctionIdListNames":[],"junctionReferenceTo":[],
                    "relationshipName":"Parts__r","restrictedDelete":false}],
                 "fields":[
                   {"name":"Id","type":"id","label":"Record ID","length":18,"nillable":false,
                    "createable":false,"updateable":false,"defaultValue":null,"referenceTo":[],
                    "picklistValues":[],"calculated":false,"externalId":false,"soapType":"tns:ID"},
                   {"name":"Status__c","type":"picklist","label":"Status","length":255,"nillable":true,
                    "createable":true,"updateable":true,"defaultValue":"New","referenceTo":[],
                    "picklistValues":[
                      {"active":true,"defaultValue":true,"label":"New","validFor":null,"value":"New"},
                      {"active":false,"defaultValue":false,"label":"Gamla år","validFor":null,"value":"Old"}]},
                   {"name":"Is_Active__c","type":"boolean","label":"Active","length":0,"nillable":false,
                    "createable":true,"updateable":true,"defaultValue":false,"referenceTo":[],"picklistValues":[]},
                   {"name":"Account__c","type":"reference","label":"Account","length":18,"nillable":true,
                    "createable":true,"updateable":true,"defaultValue":null,"referenceTo":["Account"],
                    "relationshipName":"Account__r","picklistValues":[]}]}
                """);

        DescribeSObjectResult describe = get("/services/data/" + API + "/sobjects/Widget__c/describe",
                DescribeSObjectResult.class);

        assertEquals("Widget__c", describe.name);
        assertEquals("a05", describe.keyPrefix);
        assertEquals(Boolean.TRUE, describe.replicateable);
        assertEquals(1, describe.childRelationships.size());
        assertEquals("Parts__r", describe.childRelationships.get(0).relationshipName);
        assertEquals("Widget_Part__c", describe.childRelationships.get(0).childSObject);
        assertEquals("Widget__c", describe.childRelationships.get(0).field);

        assertEquals(4, describe.fields.size());
        Field id = describe.fields.get(0);
        assertEquals("id", id.type);
        assertEquals(18, id.length);
        assertEquals(Boolean.FALSE, id.nillable);
        assertNull(id.defaultValue);

        Field status = describe.fields.get(1);
        assertEquals("New", status.defaultValue);
        assertEquals(2, status.picklistValues.size());
        assertEquals("Gamla år", status.picklistValues.get(1).label);
        assertEquals(Boolean.FALSE, status.picklistValues.get(1).active);

        assertEquals(Boolean.FALSE, describe.fields.get(2).defaultValue);
        assertEquals(List.of("Account"), describe.fields.get(3).referenceTo);
    }

    @Test
    void soqlQueryMapsBackupConfigRecords() {
        responses.put("/services/data/" + API + "/query/", """
                {"totalSize":2,"done":true,"records":[
                  {"attributes":{"type":"Heimdall_Backup_Config__c",
                                 "url":"/services/data/v61.0/sobjects/Heimdall_Backup_Config__c/a00XX000001AAAA"},
                   "Id":"a00XX000001AAAA","Name":"Account","ObjectName__c":"Account",
                   "CreatedById":"005XX000001Sv6AAE","LastModifiedById":"005XX000001Sv6AAE",
                   "Last_Backup_Run__c":"2026-10-30","Status__c":"Backup","CheckCountFirst__c":true,
                   "ConsecutiveEmptyRuns__c":0,"LastNonEmptyRun__c":"2026-10-29","ChangeFrequency__c":"Daily",
                   "AverageRecordsPerRun__c":12.5,"Archive__c":false,"Archive_Age_Days__c":null,
                   "Archive_Expression__c":null,"Archive_Action__c":null},
                  {"attributes":{"type":"Heimdall_Backup_Config__c",
                                 "url":"/services/data/v61.0/sobjects/Heimdall_Backup_Config__c/a00XX000002AAAA"},
                   "Id":"a00XX000002AAAA","Name":"Widget__c","ObjectName__c":"Widget__c",
                   "Last_Backup_Run__c":null,"Status__c":"Backup + Archive","CheckCountFirst__c":false,
                   "ConsecutiveEmptyRuns__c":3,"LastNonEmptyRun__c":null,"ChangeFrequency__c":null,
                   "AverageRecordsPerRun__c":null,"Archive__c":true,"Archive_Age_Days__c":365,
                   "Archive_Expression__c":"Status__c = 'Old'","Archive_Action__c":"Delete"}]}
                """);

        SalesforceService.SOQLResultObjectBackup result = get("/services/data/" + API + "/query/",
                SalesforceService.SOQLResultObjectBackup.class);

        assertEquals(2, result.totalSize);
        assertEquals("true", result.done);
        assertFalse(result.hasMoreRecords());
        assertEquals(2, result.records.size());

        Heimdall_Backup_Config__c account = result.records.get(0);
        assertEquals(Heimdall_Backup_Config__c.class, account.getClass());
        assertEquals("a00XX000001AAAA", account.getId());
        assertEquals("Account", account.getName());
        assertEquals("Account", account.getObjectName__c());
        assertEquals("005XX000001Sv6AAE", account.getCreatedById());
        assertEquals(Instant.parse("2026-10-30T00:00:00Z"), account.getLast_Backup_Run__c().toInstant());
        assertEquals(Instant.parse("2026-10-29T00:00:00Z"), account.getLastNonEmptyRun__c().toInstant());
        assertEquals("Backup", account.getStatus__c());
        assertEquals(Boolean.TRUE, account.getCheckCountFirst__c());
        assertEquals(0, account.getConsecutiveEmptyRuns__c());
        assertEquals(12.5, account.getAverageRecordsPerRun__c());
        assertEquals(Boolean.FALSE, account.getArchive__c());
        assertNull(account.getArchive_Age_Days__c());

        Heimdall_Backup_Config__c widget = result.records.get(1);
        assertNull(widget.getLast_Backup_Run__c());
        assertNull(widget.getAverageRecordsPerRun__c());
        assertEquals(365, widget.getArchive_Age_Days__c());
        assertEquals("Status__c = 'Old'", widget.getArchive_Expression__c());
    }

    @Test
    void soqlCountAndPagedResults() {
        responses.put("/services/data/" + API + "/query/count", """
                {"totalSize":4211,"done":true,"records":[]}
                """);
        responses.put("/services/data/" + API + "/query/paged", """
                {"totalSize":2400,"done":false,
                 "nextRecordsUrl":"/services/data/v61.0/query/01gXX0000000001AAA-2000","records":[]}
                """);

        SalesforceService.SOQLCountResult count = get("/services/data/" + API + "/query/count",
                SalesforceService.SOQLCountResult.class);
        assertEquals(4211, count.totalSize);
        assertTrue(count.done);

        SalesforceService.SOQLResultObjectBackup paged = get("/services/data/" + API + "/query/paged",
                SalesforceService.SOQLResultObjectBackup.class);
        assertEquals("false", paged.done);
        assertTrue(paged.hasMoreRecords());
        assertEquals("/services/data/v61.0/query/01gXX0000000001AAA-2000", paged.nextRecordsUrl);
    }

    @Test
    void bulkQueryJobResponses() {
        responses.put("/services/data/" + API + "/jobs/query", """
                {"id":"750XX000000AAAAAAA","operation":"queryAll","object":"Account",
                 "createdById":"005XX000001Sv6AAE","createdDate":"2026-11-03T05:03:13.000+0000",
                 "systemModstamp":"2026-11-03T05:03:13.000+0000","state":"UploadComplete",
                 "concurrencyMode":"Parallel","contentType":"CSV","apiVersion":61.0,
                 "lineEnding":"LF","columnDelimiter":"COMMA"}
                """);
        responses.put("/services/data/" + API + "/jobs/query/750XX000000AAAAAAA", """
                {"id":"750XX000000AAAAAAA","operation":"queryAll","object":"Account",
                 "createdById":"005XX000001Sv6AAE","createdDate":"2026-11-03T05:03:13.000+0000",
                 "systemModstamp":"2026-11-03T05:03:20.000+0000","state":"JobComplete",
                 "concurrencyMode":"Parallel","contentType":"CSV","apiVersion":61.0,"jobType":"V2Query",
                 "lineEnding":"LF","columnDelimiter":"COMMA","numberRecordsProcessed":4211,
                 "retries":0,"totalProcessingTime":1834,"isPkChunkingSupported":true}
                """);

        BulkQueryRequest created = get("/services/data/" + API + "/jobs/query", BulkQueryRequest.class);
        assertEquals("750XX000000AAAAAAA", created.id);
        assertEquals("queryAll", created.operation);
        assertEquals("UploadComplete", created.state);
        assertEquals("61.0", created.apiVersion);
        assertEquals("2026-11-03T05:03:13.000+0000", created.createdDate);
        assertNull(created.numberRecordsProcessed);

        BulkQueryRequest done = get("/services/data/" + API + "/jobs/query/750XX000000AAAAAAA",
                BulkQueryRequest.class);
        assertEquals("JobComplete", done.state);
        assertEquals("V2Query", done.jobType);
        assertEquals(4211, done.numberRecordsProcessed);
        assertEquals(0, done.retries);
        assertEquals(1834, done.totalProcessingTime);

        BulkQueryResult polled = get("/services/data/" + API + "/jobs/query/750XX000000AAAAAAA",
                BulkQueryResult.class);
        assertEquals("JobComplete", polled.getState());
        assertEquals("Account", polled.getObject());
        assertEquals(0, polled.getRecordCount());
        assertNull(polled.getErrorMessage());
    }

    private static SalesforceAccessToken token() {
        SalesforceAccessToken token = new SalesforceAccessToken();
        token.accessToken = "token";
        return token;
    }
}
