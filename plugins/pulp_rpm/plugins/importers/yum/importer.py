from gettext import gettext as _

from pulp.plugins.importer import Importer
from pulp.common.config import read_json_config
from pulp.server.controllers import repository as repo_controller
from pulp.server.db import model as platform_models


from pulp_rpm.common import constants, ids
from pulp_rpm.plugins.db import models
from pulp_rpm.plugins.importers.yum import (
    sync, associate, upload, config_validate, modularity, pulp_solv
)


# The platform currently doesn't support automatic loading of conf files when the plugin
# uses entry points. The current thinking is that the conf files will be named the same as
# the plugin and put in a conf.d type of location. For now, this implementation will assume
# that's the final solution and the plugin will attempt to load the file itself in the
# entry_point method.
CONF_FILENAME = 'server/plugins.conf.d/%s.json' % ids.TYPE_ID_IMPORTER_YUM


def entry_point():
    """
    Entry point that pulp platform uses to load the importer
    :return: importer class and its config
    :rtype:  Importer, {}
    """
    plugin_config = read_json_config(CONF_FILENAME)
    return YumImporter, plugin_config


class YumImporter(Importer):
    @classmethod
    def metadata(cls):
        return {
            'id': ids.TYPE_ID_IMPORTER_YUM,
            'display_name': _('Yum Importer'),
            'types': [
                models.Distribution._content_type_id.default,
                models.DRPM._content_type_id.default,
                models.Errata._content_type_id.default,
                models.PackageGroup._content_type_id.default,
                models.PackageCategory._content_type_id.default,
                models.RPM._content_type_id.default,
                models.SRPM._content_type_id.default,
                models.YumMetadataFile._content_type_id.default,
                models.PackageEnvironment._content_type_id.default,
                models.PackageLangpacks._content_type_id.default,
                models.Modulemd._content_type_id.default,
                models.ModulemdDefaults._content_type_id.default,
            ]
        }

    def validate_config(self, repo, config):
        return config_validate.validate(config)

    def import_units(self, source_transfer_repo, dest_transfer_repo, import_conduit, config,
                     units=None):
        source_repo = platform_models.Repository.objects.get(repo_id=source_transfer_repo.id)
        dest_repo = platform_models.Repository.objects.get(repo_id=dest_transfer_repo.id)

        # get config items that we care about
        recursive = config.get(constants.CONFIG_RECURSIVE) or \
            config.get(constants.CONFIG_RECURSIVE_CONSERVATIVE)

        if config.get(constants.CONFIG_ADDITIONAL_REPOS) and not recursive:
            # TODO: is there a better error to raise?
            raise ValueError("Cannot use additional_repos without one of the recursive flags set")

        if not recursive:
            return associate.associate(source_repo, dest_repo, import_conduit, config, units)

        # Everything below here only occurs when recursive=True
        # =====================================================

        additional_repos = config.get(constants.CONFIG_ADDITIONAL_REPOS, {})

        solver = pulp_solv.Solver(
            source_repo,
            target_repo=dest_repo,
            conservative=config.get(constants.CONFIG_RECURSIVE_CONSERVATIVE),
            ignore_missing=False
            # the line above disables the code which injects "dummy solvables" to provide
            # missing packages. it is not known whether that code is 100% necessary, but it
            # is feeding invalid data to libsolv somehow resulting in a violated invariant
            # and failure on an assert statement here
            # https://github.com/openSUSE/libsolv/blob/master/src/solver.c#L1979-L1981
        )
        solver.load()

        repo_unit_set = solver.find_dependent_rpms(
            [
                unit for unit in units if unit._content_type_id in
                (ids.TYPE_ID_RPM, ids.TYPE_ID_MODULEMD)
            ]
        )

        (total_copied_units, total_failed_units) = associate.associate(
            source_repo, dest_repo, import_conduit, config, repo_unit_set[source_repo.repo_id])

        for src_id, dest_id in additional_repos.items():
            additional_src = platform_models.Repository.objects.get(repo_id=src_id)
            additional_dest = platform_models.Repository.objects.get(repo_id=dest_id)
            units_to_copy = repo_unit_set.get(src_id)
            if not units_to_copy:
                continue

            (copied_units, failed_units) = associate.associate(
                additional_src, additional_dest, import_conduit, config, units_to_copy)

            total_copied_units |= copied_units
            total_failed_units |= failed_units
            if isinstance(copied_units, tuple):
                suc_units_ids = [u.to_id_dict() for u in copied_units if u is not None]
                repo_controller.rebuild_content_unit_counts(dest_repo)
                if suc_units_ids:
                    repo_controller.update_last_unit_added(dest_repo.repo_id)
            unit_ids = [u.to_id_dict() for u in copied_units if u is not None]
            repo_controller.rebuild_content_unit_counts(dest_repo)
            if unit_ids:
                repo_controller.update_last_unit_added(dest_repo.repo_id)

        # TODO: Is this valid, or are there assumptions that the copied and failed units are only
        # relating to the primary source and destination repositories?
        return (total_copied_units, total_failed_units)

    def upload_unit(self, transfer_repo, type_id, unit_key, metadata, file_path, conduit, config):
        repo = transfer_repo.repo_obj
        conduit.repo = repo
        return upload.upload(repo, type_id, unit_key, metadata, file_path, conduit, config)

    def sync_repo(self, transfer_repo, sync_conduit, call_config):
        """
        :param transfer_repo: metadata describing the repository
        :type  transfer_repo: pulp.plugins.model.Repository

        :param sync_conduit: provides access to relevant Pulp functionality
        :type  sync_conduit: pulp.plugins.conduits.repo_sync.RepoSyncConduit

        :param call_config: plugin configuration
        :type  call_config: pulp.plugins.config.PluginCallConfiguration

        :return: report of the details of the sync
        :rtype:  pulp.plugins.model.SyncReport
        """
        repo = transfer_repo.repo_obj
        sync_conduit.repo = repo
        self._current_sync = sync.RepoSync(repo, sync_conduit, call_config)
        report = self._current_sync.run()
        return report

    def remove_units(self, transfer_repo, units, call_config):
        """
        Remove units which require plugin specific handling.

        Remove Modulemd content units and modular RPMs which belong to them.
        Other content unit types are removed by the core in a standard way.

        :param transfer_repo:   metadata describing the repository
        :type  trnasfer_repo:   pulp.plugins.model.Repository
        :param units:  list of objects describing the units to remove
        :type  units:  list of pulp.server.db.model.ContentUnit
        :param call_config: plugin configuration
        :type  call_config: pulp.plugins.config.PluginCallConfiguration
        """
        repo = transfer_repo.repo_obj
        modulemds_to_remove = set(unit.unit_key_as_named_tuple for unit in units
                                  if isinstance(unit, models.Modulemd))
        if modulemds_to_remove:
            modularity.remove_modulemds(repo, modulemds_to_remove)
