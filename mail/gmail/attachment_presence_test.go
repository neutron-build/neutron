package gmail

import (
	"github.com/neutron-build/neutron/mail"
	api "google.golang.org/api/gmail/v1"
	"testing"
)

func TestMetadataAttachmentPresenceIsUnknown(t *testing.T) {
	env := toEnvelope(&api.Message{Id: "metadata", Payload: &api.MessagePart{Headers: []*api.MessagePartHeader{{Name: "Subject", Value: "with omitted attachment tree"}}}})
	if env.HasAttachment || env.AttachmentPresence != mail.AttachmentUnknown {
		t.Fatal("metadata asserted absence", env)
	}
}
