# frozen_string_literal: true

require 'spec_helper'
require 'tempfile'
require 'clickhouse-activerecord/tasks'

RSpec.describe 'Structure dump', :migrations do
  let(:connection) { ActiveRecord::Base.connection }
  let(:configuration) { ActiveRecord::Base.connection_db_config }
  let(:tasks) { ClickhouseActiverecord::Tasks.new(configuration) }

  def dump_to_string
    file = Tempfile.new(['structure', '.sql'])
    tasks.structure_dump(file.path)
    File.read(file.path)
  ensure
    file&.close
    file&.unlink
  end

  # Names chosen so a naive flat alphabetical sort would place the materialized
  # view ('aaa_dump_order_mv') *before* its target table ('zzz_dump_order_target').
  # A materialized view created with `TO <table>` requires the target table to
  # already exist, so the dump must emit tables before materialized views.
  describe 'ordering of materialized views relative to their target tables' do
    before do
      connection.execute(<<~SQL)
        CREATE TABLE zzz_dump_order_source (id UInt64) ENGINE = MergeTree ORDER BY id
      SQL
      connection.execute(<<~SQL)
        CREATE TABLE zzz_dump_order_target (id UInt64) ENGINE = MergeTree ORDER BY id
      SQL
      connection.execute(<<~SQL)
        CREATE MATERIALIZED VIEW aaa_dump_order_mv TO zzz_dump_order_target
        AS SELECT id FROM zzz_dump_order_source
      SQL
    end

    after do
      connection.execute('DROP VIEW IF EXISTS aaa_dump_order_mv')
      connection.execute('DROP TABLE IF EXISTS zzz_dump_order_target')
      connection.execute('DROP TABLE IF EXISTS zzz_dump_order_source')
    end

    it 'emits the target table before the materialized view' do
      sql = dump_to_string

      target_index = sql.index('CREATE TABLE zzz_dump_order_target')
      mv_index = sql.index('CREATE MATERIALIZED VIEW aaa_dump_order_mv')

      expect(target_index).not_to be_nil
      expect(mv_index).not_to be_nil
      expect(target_index).to be < mv_index
    end

    it 'produces a dump that loads without UNKNOWN_TABLE errors' do
      sql = dump_to_string

      connection.execute('DROP VIEW IF EXISTS aaa_dump_order_mv')
      connection.execute('DROP TABLE IF EXISTS zzz_dump_order_target')
      connection.execute('DROP TABLE IF EXISTS zzz_dump_order_source')

      expect do
        sql.split(";\n\n").each do |statement|
          next if statement.gsub(/[a-z]/i, '').blank?

          connection.execute(statement)
        end
      end.not_to raise_error
    end
  end
end
