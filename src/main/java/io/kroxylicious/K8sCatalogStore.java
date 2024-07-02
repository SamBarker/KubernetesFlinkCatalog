package io.kroxylicious;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

public class K8sCatalogStore implements CatalogStore {
    private final CustomResourceDefinitionContext flinkCatalogResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
            .withGroup("test.fabric8.io")
            .withName("flinkCatalog.test.fabric8.io")
            .withPlural("catalogs")
            .withScope("Namespaced")
            .withVersion("v1alpha1")
            .build();

    private final KubernetesClient kubernetesClient;
    private final String namespace;

    public K8sCatalogStore(KubernetesClient kubernetesClient, String namespace) {
        this.kubernetesClient = kubernetesClient;
        this.namespace = namespace;
    }

    @Override
    public void storeCatalog(String s, CatalogDescriptor catalogDescriptor) throws CatalogException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void removeCatalog(String s, boolean b) throws CatalogException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
        final GenericKubernetesResource genericKubernetesResource = kubernetesClient.genericKubernetesResources(flinkCatalogResourceDefinitionContext)
                .inNamespace(namespace).withName(catalogName).get();
        if (Objects.isNull(genericKubernetesResource)) {
            return Optional.empty();
        }
        else {
            return Optional.of(new FlinkCatalog(genericKubernetesResource).toCatalogDescriptor());
        }
    }

    @Override
    public Set<String> listCatalogs() throws CatalogException {
        return Set.of();
    }

    @Override
    public boolean contains(String s) throws CatalogException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void open() throws CatalogException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void close() throws CatalogException {
        throw new UnsupportedOperationException("TODO");
    }
}
