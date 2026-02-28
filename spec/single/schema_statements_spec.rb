# frozen_string_literal: true

RSpec.describe 'ActiveRecord::ConnectionAdapters::Clickhouse::SchemaStatements' do
  let(:connection) { ActiveRecord::Base.connection }

  describe '#internal_exec_query' do
    it 'uses adapter default response format when scoped format is nil' do
      result = nil

      expect do
        connection.with_response_format(nil) do
          result = connection.select_all('SELECT 1 AS one')
        end
      end.not_to raise_error

      expect(result.to_a).to eq([{ 'one' => 1 }])
    end
  end

  describe '#tables' do
    before do
      connection.execute('CREATE TABLE tables_format_test (id UInt64) ENGINE = MergeTree ORDER BY id')
    end

    after do
      connection.execute('DROP TABLE IF EXISTS tables_format_test')
    end

    it 'returns tables even when response format is scoped to nil' do
      result = nil

      expect do
        connection.with_response_format(nil) do
          result = connection.tables
        end
      end.not_to raise_error

      expect(result).to include('tables_format_test')
    end
  end

  describe '#truncate_tables' do
    before do
      connection.execute('CREATE TABLE truncate_test (id UInt64, name String) ENGINE = MergeTree ORDER BY id')
    end

    after do
      connection.execute('DROP DICTIONARY IF EXISTS truncate_test_dict')
      connection.execute('DROP TABLE IF EXISTS truncate_test')
      connection.execute('DROP TABLE IF EXISTS truncate_test2')
      connection.execute('DROP VIEW IF EXISTS truncate_test_view')
    end

    it 'truncates multiple tables' do
      connection.execute('CREATE TABLE truncate_test2 (id UInt64, value Int32) ENGINE = MergeTree ORDER BY id')
      connection.exec_insert("INSERT INTO truncate_test (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
      connection.exec_insert("INSERT INTO truncate_test2 (id, value) VALUES (1, 100), (2, 200)")

      expect(connection.select_value('SELECT count() FROM truncate_test').to_i).to eq(2)
      expect(connection.select_value('SELECT count() FROM truncate_test2').to_i).to eq(2)

      connection.truncate_tables('truncate_test', 'truncate_test2')

      expect(connection.select_value('SELECT count() FROM truncate_test').to_i).to eq(0)
      expect(connection.select_value('SELECT count() FROM truncate_test2').to_i).to eq(0)
    end

    it 'skips tables with unsupported engines' do
      connection.execute('CREATE VIEW truncate_test_view AS SELECT * FROM truncate_test')
      connection.execute(<<~SQL)
        CREATE DICTIONARY truncate_test_dict (
          id UInt64,
          name String
        )
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'truncate_test'))
        LIFETIME(MIN 0 MAX 0)
        LAYOUT(FLAT())
      SQL
      connection.exec_insert("INSERT INTO truncate_test (id, name) VALUES (1, 'Alice')")

      expect { connection.truncate_tables(*connection.tables) }.not_to raise_error

      expect(connection.select_value('SELECT count() FROM truncate_test').to_i).to eq(0)
    end
  end
end
