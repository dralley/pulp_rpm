from pulpcore.plugin.viewsets import (
    ContentFilter,
    NoArtifactContentUploadViewSet,
)

from pulp_rpm.app.models import (
    UpdateRecord,
)
from pulp_rpm.app.serializers import (
    MinimalUpdateRecordSerializer,
    UpdateRecordSerializer,
)


class UpdateRecordFilter(ContentFilter):
    """
    FilterSet for UpdateRecord.
    """

    class Meta:
        model = UpdateRecord
        fields = {
            "id": ["exact", "in"],
            "status": ["exact", "in", "ne"],
            "severity": ["exact", "in", "ne"],
            "type": ["exact", "in", "ne"],
        }


class UpdateRecordViewSet(NoArtifactContentUploadViewSet):
    """
    A ViewSet for UpdateRecord.

    Define endpoint name which will appear in the API endpoint for this content type.
    For example::
        http://pulp.example.com/pulp/api/v3/content/rpm/advisories/

    Also specify queryset and serializer for UpdateRecord.
    """

    endpoint_name = "advisories"
    queryset = UpdateRecord.objects.all()
    serializer_class = UpdateRecordSerializer
    minimal_serializer_class = MinimalUpdateRecordSerializer
    filterset_class = UpdateRecordFilter

    # TODO: adjust this policy after upload access policy design done and in place
    DEFAULT_ACCESS_POLICY = {
        "statements": [
            {
                "action": ["list", "retrieve"],
                "principal": "authenticated",
                "effect": "allow",
            },
            {
                "action": ["create"],
                "principal": "authenticated",
                "effect": "allow",
                "condition": [
                    "has_required_repo_perms_on_upload:rpm.modify_content_rpmrepository",
                    "has_required_repo_perms_on_upload:rpm.view_rpmrepository",
                ],
            },
            {
                "action": ["set_label", "unset_label"],
                "principal": "authenticated",
                "effect": "allow",
                "condition": [
                    "has_model_or_domain_perms:core.manage_content_labels",
                ],
            },
        ],
        "queryset_scoping": {"function": "scope_queryset"},
    }
