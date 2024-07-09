import java.net.HttpURLConnection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.table.catalog.CatalogDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;

import io.kroxylicious.FlinkCatalog;
import io.kroxylicious.K8sCatalogStore;

import static org.assertj.core.api.Assertions.assertThat;

@EnableKubernetesMockClient(crud = true)
class K8sCatalogStoreTest {

    public static final String HELLO_CATALOG = "hello-catalog";
    KubernetesMockServer server;

    private K8sCatalogStore catalogStore;
    private static final String NAMESPACE = "ns1";
    private static final String BASE_CATALOGS_PATH = "/apis/com.redhat.s4ak/v1alpha1/namespaces/" + NAMESPACE + "/flinkcatalogs";

    @BeforeEach
    void setUp() {
        catalogStore = new K8sCatalogStore(server.createClient(), NAMESPACE);
    }

    @Test
    void shouldReturnEmptyForUnknownCatalog() {
        // Given

        // When
        final Optional<CatalogDescriptor> catalog = catalogStore.getCatalog("unknown");

        // Then
        assertThat(catalog).isEmpty();
    }

    @Test
    void shouldReturnEmptySetIfNoCatalogsExist() {
        // Given

        // When
        final Set<String> catalogs = catalogStore.listCatalogs();

        // Then
        assertThat(catalogs).isEmpty();
    }

    @Test
    void shouldLoadCatalogResource() {
        // Given
        String jsonObject = "{\"apiVersion\": \"test.fabric8.io/v1alpha1\",\"kind\": \"Hello\"," +
                "\"metadata\": {\"resourceVersion\":\"1\", \"name\": \"" + HELLO_CATALOG + "\"},\"spec\": {\"size\": 3}}";
        server.expect().get().withPath(BASE_CATALOGS_PATH + "/hello-catalog")
                .andReturn(HttpURLConnection.HTTP_OK, jsonObject).once();
        final int initialRequestCount = server.getRequestCount();

        // When
        final Optional<CatalogDescriptor> catalog = catalogStore.getCatalog(HELLO_CATALOG);

        // Then
        assertThat(server.getRequestCount()).isGreaterThanOrEqualTo(initialRequestCount + 1); //Ensure we made at least one request
        assertThat(catalog).isNotEmpty().get().extracting(CatalogDescriptor::getCatalogName).isEqualTo(HELLO_CATALOG);
    }

    @Test
    void shouldListFlinkCatalogResources() {
        // Given
        // note the mock server does exact path matching so XXX/ and XXX are considered different paths. The client however strips trailing slashes so maps XXX/ to XXX
        // A serious trap for unwary players
        server.expect().get().withPath(BASE_CATALOGS_PATH)
                .andReturn(HttpURLConnection.HTTP_OK,
                        new FlinkCatalog.FlinkCatalogList(
                                List.of(new FlinkCatalog("example-hello"),
                                        new FlinkCatalog("the-other-catalog"))))
                .once();
        final int initialRequestCount = server.getRequestCount();

        // When
        final Set<String> catalogs = catalogStore.listCatalogs();

        // Then
        assertThat(server.getRequestCount()).isGreaterThanOrEqualTo(initialRequestCount + 1); //Ensure we made at least one request
        assertThat(catalogs).hasSize(2);
    }

    @Test
    void shouldReturnFalseForContainsWhenCatalogDoesntExist() {
        // Given
        server.expect().get().withPath(BASE_CATALOGS_PATH)
                .andReturn(HttpURLConnection.HTTP_OK,
                        new FlinkCatalog.FlinkCatalogList(
                                List.of(new FlinkCatalog("example-hello"),
                                        new FlinkCatalog("the-other-catalog"))))
                .once();

        // When
        final boolean contained = catalogStore.contains("unknown");

        // Then
        assertThat(contained).isFalse();
    }

    @Test
    void shouldReturnTrueForContainsWhenCatalogDoesntExist() {
        // Given
        server.expect().get().withPath(BASE_CATALOGS_PATH)
                .andReturn(HttpURLConnection.HTTP_OK,
                        new FlinkCatalog.FlinkCatalogList(
                                List.of(new FlinkCatalog("example-hello"),
                                        new FlinkCatalog("the-other-catalog"))))
                .once();

        // When
        final boolean contained = catalogStore.contains("example-hello");

        // Then
        assertThat(contained).isTrue();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.shutdown();
        }
    }
}
