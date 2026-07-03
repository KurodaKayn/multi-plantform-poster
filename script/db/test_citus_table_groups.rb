# frozen_string_literal: true

require "minitest/autorun"
require "yaml"

class CitusTableGroupsTest < Minitest::Test
  DESIGN_PATH = File.expand_path("citus_table_groups.yml", __dir__)

  REQUIRED_COLOCATED_TABLES = %w[
    workspaces
    workspace_members
    projects
    project_platform_publications
    project_list_summaries
    project_activities
    project_comments
    project_versions
    project_share_links
    scheduled_publications
    publish_events
    extension_callback_tokens
    extension_execution_events
    media_assets
    media_asset_usages
    brand_profiles
    collab_documents
    collab_document_states
    collab_document_update_batches
    ai_context_snapshots
    ai_growth_optimization_runs
    ai_proposals
    ai_drafting_sessions
  ].freeze

  REQUIRED_CONTROL_TABLES = %w[
    platform_accounts
    platform_account_grants
    remote_browser_sessions
    outbox_events
    extension_execution_event_claims
  ].freeze

  REQUIRED_DEFERRED_TABLES = %w[
    project_collaborators
    collab_document_collaborators
    publish_attempts
    content_templates
    ai_drafting_messages
    ai_tool_calls
    ai_drafting_session_summaries
    ai_session_events
  ].freeze

  def setup
    @design = YAML.safe_load_file(DESIGN_PATH, aliases: false)
  end

  def test_manifest_declares_workspace_distribution
    assert_equal 1, @design.fetch("version")
    assert_equal "workspace_id", @design.fetch("distribution_key")

    group = tenant_workspace_group
    assert_equal "hash", group.fetch("distribution_type")
    assert_equal "workspace_uuid", group.fetch("distribution_value")
    assert_equal "workspaces", group.fetch("colocate_with")
  end

  def test_colocated_tables_use_workspace_distribution
    colocated_tables.each do |entry|
      table = entry.fetch("table")
      expected_column = table == "workspaces" ? "id" : "workspace_id"

      assert_equal expected_column, entry.fetch("distribution_column"), table
      assert_includes entry.fetch("ddl"), "create_distributed_table", table
      assert entry.fetch("domain").length.positive?, table
      assert entry.fetch("readiness").length.positive?, table
      assert entry.fetch("notes").length.positive?, table
    end
  end

  def test_design_covers_phase_five_anchor_tables
    missing_colocated = REQUIRED_COLOCATED_TABLES - table_names(colocated_tables)
    missing_control = REQUIRED_CONTROL_TABLES - table_names(@design.fetch("control_domain_tables"))
    missing_deferred = REQUIRED_DEFERRED_TABLES - table_names(@design.fetch("deferred_colocation_tables"))

    assert_empty missing_colocated, "missing colocated tables: #{missing_colocated.join(", ")}"
    assert_empty missing_control, "missing control-domain tables: #{missing_control.join(", ")}"
    assert_empty missing_deferred, "missing deferred tables: #{missing_deferred.join(", ")}"
  end

  def test_reference_tables_use_reference_ddl
    reference_tables = @design.fetch("reference_tables")

    refute_empty reference_tables
    reference_tables.each do |entry|
      assert_includes entry.fetch("ddl"), "create_reference_table", entry.fetch("table")
      assert entry.fetch("notes").length.positive?, entry.fetch("table")
    end
  end

  def test_every_table_has_one_classification
    all_tables = table_names(colocated_tables) +
      table_names(@design.fetch("reference_tables")) +
      table_names(@design.fetch("control_domain_tables")) +
      table_names(@design.fetch("deferred_colocation_tables"))

    duplicates = all_tables.tally.select { |_table, count| count > 1 }.keys

    assert_empty duplicates, "tables classified more than once: #{duplicates.join(", ")}"
  end

  def test_non_distributed_entries_explain_why_they_are_not_colocated
    @design.fetch("control_domain_tables").each do |entry|
      assert entry.fetch("reason").length.positive?, entry.fetch("table")
    end
    @design.fetch("deferred_colocation_tables").each do |entry|
      assert entry.fetch("required_change").length.positive?, entry.fetch("table")
    end
  end

  private

  def tenant_workspace_group
    @design.fetch("physical_colocation_groups").fetch("tenant_workspace")
  end

  def colocated_tables
    tenant_workspace_group.fetch("tables")
  end

  def table_names(entries)
    entries.map { |entry| entry.fetch("table") }
  end
end
