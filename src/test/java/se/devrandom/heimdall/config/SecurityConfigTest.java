package se.devrandom.heimdall.config;

import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import se.devrandom.heimdall.storage.S3Service;
import se.devrandom.heimdall.web.RestoreService;
import se.devrandom.heimdall.web.RetentionService;
import se.devrandom.heimdall.web.WebController;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies that mutating endpoints require a CSRF token and that the token
 * rendered into the page by layout.html is accepted.
 */
@WebMvcTest(WebController.class)
@Import(SecurityConfig.class)
// application.properties defaults to the batch job (no web server); the GUI runs as a servlet app
@TestPropertySource(properties = "spring.main.web-application-type=servlet")
class SecurityConfigTest {

    private static final String CLEANUP_BODY = "{\"months\": 12}";
    private static final Pattern META = Pattern.compile("<meta name=\"(_csrf(?:_header)?)\" content=\"([^\"]+)\">");

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private RestoreService restoreService;

    @MockBean
    private S3Service s3Service;

    @MockBean
    private RetentionService retentionService;

    @Test
    void postWithoutCsrfToken_isForbidden() throws Exception {
        mockMvc.perform(post("/api/cleanup/execute")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CLEANUP_BODY))
                .andExpect(status().isForbidden());
    }

    @Test
    void getRequests_doNotNeedToken() throws Exception {
        mockMvc.perform(get("/health")).andExpect(status().isOk());
    }

    @Test
    void tokenRenderedInPage_isAcceptedOnPost() throws Exception {
        MvcResult page = mockMvc.perform(get("/cleanup")).andExpect(status().isOk()).andReturn();

        // CookieCsrfTokenRepository writes a raw Set-Cookie header, which MockMvc doesn't parse into getCookies()
        String setCookie = page.getResponse().getHeaders("Set-Cookie").stream()
                .filter(h -> h.startsWith("XSRF-TOKEN=")).findFirst().orElse(null);
        assertNotNull(setCookie, "CSRF cookie should be set when a page is rendered");
        assertTrue(setCookie.contains("HttpOnly"), "CSRF cookie should not be readable from JavaScript");
        Cookie cookie = new Cookie("XSRF-TOKEN", setCookie.substring("XSRF-TOKEN=".length(), setCookie.indexOf(';')));

        String html = page.getResponse().getContentAsString();
        String token = null;
        String headerName = null;
        Matcher m = META.matcher(html);
        while (m.find()) {
            if ("_csrf".equals(m.group(1))) token = m.group(2);
            else headerName = m.group(2);
        }
        assertNotNull(token, "layout.html should render the _csrf meta tag");
        assertEquals("X-XSRF-TOKEN", headerName);

        mockMvc.perform(post("/api/cleanup/execute")
                        .cookie(cookie)
                        .header(headerName, token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CLEANUP_BODY))
                .andExpect(status().isOk());

        mockMvc.perform(post("/api/cleanup/execute")
                        .cookie(cookie)
                        .header(headerName, "not-the-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CLEANUP_BODY))
                .andExpect(status().isForbidden());
    }
}
