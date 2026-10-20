/* Offline reproducer for cyclors 0.2.7's bundled CycloneDDS.
 * Link with cyclocut/src/cdds_blob.c and that exact libddsc.
 * Uses loopback-only domain 219; no robot topics or commands.
 * Run with `leak` to reproduce the old ownership pattern, or `fixed`.
 */
#include <dds/dds.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

extern dds_entity_t cdds_create_blob_topic(dds_entity_t, char *, char *, bool);

int main(int argc, char **argv) {
  bool fixed = argc == 2 && strcmp(argv[1], "fixed") == 0;
  const char *xml = "<CycloneDDS><Domain><General><Interfaces>"
    "<NetworkInterface name=\"lo\"/></Interfaces><AllowMulticast>false</AllowMulticast>"
    "</General><Discovery><ParticipantIndex>none</ParticipantIndex></Discovery>"
    "<Tracing><Verbosity>warning</Verbosity></Tracing></Domain></CycloneDDS>";
  dds_entity_t domain = dds_create_domain(219, xml);
  dds_entity_t participant = dds_create_participant(219, NULL, NULL);
  if (domain < 0 || participant < 0) return 2;
  for (int i = 0; i < 20000; ++i) {
    dds_entity_t topic = cdds_create_blob_topic(participant,
      "rt/offline_topic_lifetime", "std_msgs::msg::dds_::String_", true);
    if (topic < 0) {
      printf("mode=%s topic_failed_at=%d code=%d children=%d\n",
        fixed ? "fixed" : "leak", i, topic, dds_get_children(participant, NULL, 0));
      return fixed ? 1 : 0;
    }
    dds_entity_t endpoint = (i % 2) ? dds_create_writer(participant, topic, NULL, NULL)
                                  : dds_create_reader(participant, topic, NULL, NULL);
    if (endpoint < 0) {
      printf("endpoint_failed_at=%d code=%d\n", i, endpoint);
      return fixed ? 1 : 0;
    }
    /* Endpoint keeps its own topic reference. Releasing the creator's
       reference must neither delete nor invalidate the endpoint. */
    if (fixed && dds_delete(topic) != DDS_RETCODE_OK) return 3;
    dds_guid_t guid;
    if (dds_get_guid(endpoint, &guid) != DDS_RETCODE_OK) return 4;
    if (dds_delete(endpoint) != DDS_RETCODE_OK) return 5;
    if (fixed && dds_get_children(participant, NULL, 0) != 0) return 6;
  }
  printf("mode=%s completed=20000 children=%d\n", fixed ? "fixed" : "leak",
    dds_get_children(participant, NULL, 0));
  dds_delete(participant);
  dds_delete(domain);
  return fixed ? 0 : 7;
}
