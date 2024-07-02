package io.kroxylicious;

import java.net.HttpURLConnection;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.table.catalog.CatalogDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;

import static org.assertj.core.api.Assertions.assertThat;

@EnableKubernetesMockClient(crud = true)
class K8sCatalogStoreTest {

    public static final String HELLO_CATALOG = "hello-catalog";
    KubernetesMockServer server;

    private K8sCatalogStore catalogStore;
    private static final String NAMESPACE = "ns1";

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
        server.expect().get().withPath("/apis/test.fabric8.io/v1alpha1/namespaces/" + NAMESPACE + "/catalogs/hello-catalog")
                .andReturn(HttpURLConnection.HTTP_OK, jsonObject).once();
        final int initialRequestCount = server.getRequestCount();

        // When
        final Optional<CatalogDescriptor> catalog = catalogStore.getCatalog(HELLO_CATALOG);

        // Then
        assertThat(server.getRequestCount()).isGreaterThanOrEqualTo(initialRequestCount + 1); //Ensure we made at least one request
        assertThat(catalog).isNotEmpty().get().extracting(CatalogDescriptor::getCatalogName).isEqualTo(HELLO_CATALOG);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.shutdown();
        }
    }
}
