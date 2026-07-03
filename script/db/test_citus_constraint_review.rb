# frozen_string_literal: true

require "minitest/autorun"
require "yaml"

class CitusConstraintReviewTest < Minitest::Test
  REVIEW_PATH = File.expand_path("citus_constraint_review.yml", __dir__)
  GROUPS_PATH = File.expand_path("citus_table_groups.yml", __dir__)

  REQUIRED_UNIQUE_REFACTOR_TABLES = %w[
    projects
    project_platform_publications
    project_list_summaries
    project_share_links
    workspace_invites
    media_assets
    media_asset_usages
    extension_callback_tokens
    extension_execution_event_claims
  ].freeze

  REQUIRED_PARTITION_TABLES = %w[
    publish_events
    extension_execution_events
    project_activities
    workspace_activities
    collab_document_update_batches
  ].freeze

  REQUIRED_WORKER_TASKS = %w[
    publish:project
  ].freeze

  def setup
    @review = YAML.safe_load_file(REVIEW_PATH, aliases: false)
    @groups = YAML.safe_load_file(GROUPS_PATH, aliases: false)
  end

  def test_review_declares_distribution_scope
    assert_equal 1, @review.fetch("version")
    assert_equal "workspace_id", @review.fetch("distribution_key")
    refute_empty @review.fetch("review_scope").fetch("models")
    assert_equal "script/db/citus_table_groups.yml", @review.fetch("review_scope").fetch("table_group_manifest")
  end

  def test_unique_constraint_review_names_required_refactors
    reviewed = table_names(@review.fetch("unique_constraints").fetch("requires_refactor"))

    missing = REQUIRED_UNIQUE_REFACTOR_TABLES - reviewed

    assert_empty missing, "missing unique constraint review entries: #{missing.join(", ")}"
  end

  def test_partition_review_names_workspace_strategy_gaps
    reviewed = table_names(@review.fetch("partitioned_tables").fetch("require_workspace_partition_strategy"))

    missing = REQUIRED_PARTITION_TABLES - reviewed

    assert_empty missing, "missing partition review entries: #{missing.join(", ")}"
  end

  def test_foreign_key_and_join_review_has_actionable_decisions
    foreign_keys = @review.fetch("foreign_keys")
    cross_tenant_joins = @review.fetch("cross_tenant_joins")

    refute_empty foreign_keys.fetch("colocated_ready")
    refute_empty foreign_keys.fetch("reference_table_dependencies")
    refute_empty foreign_keys.fetch("control_domain_boundaries")
    refute_empty foreign_keys.fetch("deferred_until_workspace_key")
    refute_empty cross_tenant_joins.fetch("shard_local")
    refute_empty cross_tenant_joins.fetch("reference_or_control")
    refute_empty cross_tenant_joins.fetch("aggregate_offline")
    refute_empty cross_tenant_joins.fetch("prohibited_core_paths")
  end

  def test_worker_payload_review_names_tenant_routed_tasks
    reviewed_tasks = @review.fetch("worker_payloads").fetch("tenant_routed").map { |entry| entry.fetch("task_type") }

    missing = REQUIRED_WORKER_TASKS - reviewed_tasks

    assert_empty missing, "missing worker payload routing entries: #{missing.join(", ")}"
  end

  def test_table_group_manifest_uses_reviewed_readiness_terms
    colocated_tables.each do |entry|
      readiness = entry.fetch("readiness")

      refute_match(/after_.*review/, readiness, entry.fetch("table"))
    end
  end

  private

  def table_names(entries)
    entries.map { |entry| entry.fetch("table") }
  end

  def colocated_tables
    @groups.fetch("physical_colocation_groups").fetch("tenant_workspace").fetch("tables")
  end
end
