package io.kroxylicious;

import java.util.List;
import java.util.Objects;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogDescriptor;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.fabric8.generator.annotation.Required;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.ListMetaBuilder;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("com.redhat.s4ak")
@Version("v1alpha1")
@ShortNames("catalog")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FlinkCatalog extends CustomResource<FlinkCatalog.FlinkCatalogSpec, FlinkCatalog.FlinkCatalogStaus> implements Namespaced {

    private String name;

    /* for fabric8 to use reflectively reflection*/
    public FlinkCatalog() {
    }

    public FlinkCatalog(String name) {
        this.name = name;
        //TODO is this a sane thing todo?
        final ObjectMeta metadata = getMetadata();
        if(metadata == null){
            super.setMetadata(new ObjectMetaBuilder().withName(name).build());
        } else {
            metadata.setName(name);
        }
    }

    public FlinkCatalog(GenericKubernetesResource genericKubernetesResource) {
        name = genericKubernetesResource.getMetadata().getName();
    }

    public String getName() {
        return name;
    }

    public CatalogDescriptor toCatalogDescriptor() {
        return CatalogDescriptor.of(name, new Configuration());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        FlinkCatalog that = (FlinkCatalog) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name);
    }

    public static class FlinkCatalogSpec {
        @Required
        String name;
    }

    public static class FlinkCatalogStaus {

    }


    public static class FlinkCatalogList implements KubernetesResourceList<FlinkCatalog> {
        private final List<FlinkCatalog> catalogs;

        public FlinkCatalogList(List<FlinkCatalog> catalogs) {
            this.catalogs = catalogs;
        }

        @Override
        public ListMeta getMetadata() {
            return new ListMetaBuilder().build();
        }

        @Override
        public List<FlinkCatalog> getItems() {
            return catalogs;
        }
    }
}
