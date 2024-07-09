package io.kroxylicious;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

public class K8sCatalogStore implements CatalogStore {
    public static final CustomResourceDefinition CATALOG_RESOURCE_DEFINITION = CustomResourceDefinitionContext.v1CRDFromCustomResourceType(
                    FlinkCatalog.class)
            .build();
    private static final CustomResourceDefinitionContext FLINK_CATALOG_RESOURCE_DEFINITION_CONTEXT = CustomResourceDefinitionContext.fromCrd(CATALOG_RESOURCE_DEFINITION);

    private final KubernetesClient kubernetesClient;
    private final String namespace;

    public K8sCatalogStore(KubernetesClient kubernetesClient, String namespace) {
        this.kubernetesClient = kubernetesClient;
        this.namespace = namespace;
    }

    @Override
    public void storeCatalog(String catalogName, CatalogDescriptor catalogDescriptor) throws CatalogException {
        kubernetesClient.resource(new FlinkCatalog(catalogName)).inNamespace(namespace).create();
    }

    @Override
    public void removeCatalog(String s, boolean b) throws CatalogException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
        final GenericKubernetesResource genericKubernetesResource = kubernetesClient.genericKubernetesResources(FLINK_CATALOG_RESOURCE_DEFINITION_CONTEXT)
                .inNamespace(namespace)
                .withName(catalogName)
                .get();
        if (Objects.isNull(genericKubernetesResource)) {
            return Optional.empty();
        }
        else {
            return Optional.of(new FlinkCatalog(genericKubernetesResource).toCatalogDescriptor());
        }
    }

    @Override
    public Set<String> listCatalogs() throws CatalogException {
        final GenericKubernetesResourceList genericKubernetesResource = kubernetesClient.genericKubernetesResources(FLINK_CATALOG_RESOURCE_DEFINITION_CONTEXT)
                .inNamespace(namespace)
                .list();
        return genericKubernetesResource.getItems().stream().map(GenericKubernetesResource::getMetadata).map(ObjectMeta::getName).collect(Collectors.toSet());
    }

    @Override
    public boolean contains(String s) throws CatalogException {
        return listCatalogs().contains(s);
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
