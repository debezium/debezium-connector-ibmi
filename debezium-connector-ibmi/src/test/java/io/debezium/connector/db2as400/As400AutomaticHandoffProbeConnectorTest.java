/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaTopicNamingStrategy;

class As400AutomaticHandoffProbeConnectorTest {
    @Test
    void shouldRejectForeignOffsetChangesWhenConnectorIsStopped() {
        var connector = new As400AutomaticHandoffProbeConnector();
        var config = Map.of("name", "automatic", "topic.prefix", "test");
        assertThat(connector.config().validate(config)).allSatisfy(value -> assertThat(value.errorMessages()).isEmpty());
        assertThat(connector.alterOffsets(config, Map.of(Map.of("server", "test"), Map.of()))).isTrue();
        assertThatThrownBy(() -> connector.alterOffsets(config, Map.of(Map.of("server", "synthetic"), Map.of())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("isolated automatic test partition");
        assertThatThrownBy(() -> connector.alterOffsets(Map.of("name", "another", "topic.prefix", "test"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNativeWorkWhenIsolatedTestAdmissionIsAbsent() {
        var task = new As400AutomaticHandoffProbeConnector.AutomaticTask();
        assertThatThrownBy(() -> task.recoveryForTesting(Configuration.create().with("database.schema", "DCNC9999").build()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Isolated automatic admission failed")
                .hasRootCauseMessage("Explicit isolated automatic task required");
    }

    @Test
    void shouldPreservePhysicalIdentityAndTopicWhenCatalogBindingIsRestored() throws Exception {
        var acquired = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions();
        var nativeBinding = acquired.get(0);
        var qualified = As400AutomaticHandoffProbeConnector.legacyCatalog(acquired, "DB").get(0);
        var saved = As400BindingDefinition.restore(qualified.storedTable());
        assertThat(saved).isEqualTo(qualified);
        assertThat(saved.identity().fileJournalId()).isEqualTo(nativeBinding.identity().fileJournalId());
        assertThat(saved.identity().memberJournalId()).isEqualTo(nativeBinding.identity().memberJournalId());
        assertThat(saved.identity().recordFormat()).isEqualTo(nativeBinding.identity().recordFormat());
        assertThat(saved.codecs()).isEqualTo(nativeBinding.codecs());
        assertThat(saved.schema().columns()).isEqualTo(nativeBinding.schema().columns());
        assertThat(saved.schema().primaryKeyColumnNames()).isEqualTo(nativeBinding.schema().primaryKeyColumnNames());
        var naming = new SchemaTopicNamingStrategy(Configuration.create().with("topic.prefix", "test").build().asProperties(), true);
        var legacy = new TableId("DB", nativeBinding.schema().id().schema(), nativeBinding.schema().id().table());
        assertThat(saved.schema().id()).isEqualTo(legacy);
        assertThat(naming.dataChangeTopic(saved.schema().id())).isEqualTo(naming.dataChangeTopic(legacy));
        assertThat(naming.dataChangeTopic(nativeBinding.schema().id())).isNotEqualTo(naming.dataChangeTopic(legacy));
        assertThatThrownBy(() -> As400AutomaticHandoffProbeConnector.legacyCatalog(java.util.List.of(saved), "OTHER"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("catalog association");
    }
}
