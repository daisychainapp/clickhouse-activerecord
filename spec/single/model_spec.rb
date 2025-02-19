# frozen_string_literal: true

RSpec.describe 'Model', :migrations do

  class ModelJoin < ActiveRecord::Base
    self.table_name = 'joins'
    belongs_to :model, class_name: 'Model'
  end
  class Model < ActiveRecord::Base
    self.table_name = 'sample'
    has_many :joins, class_name: 'ModelJoin', primary_key: 'event_name'
  end
  class ModelPk < ActiveRecord::Base
    self.table_name = 'sample'
    self.primary_key = 'event_name'
  end
  IS_NEW_CLICKHOUSE_SERVER = Model.connection.server_version.to_f >= 23.4

  let(:date) { Date.today }

  context 'sample' do

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    if IS_NEW_CLICKHOUSE_SERVER
      it "detect primary key" do
        expect(Model.primary_key).to eq('event_name')
      end
    end

    it 'DB::Exception in row value' do
      Model.create!(event_name: 'DB::Exception')
      expect(Model.first.event_name).to eq('DB::Exception')
    end

    describe '#do_execute' do
      it 'returns formatted result' do
        result = Model.connection.do_execute('SELECT 1 AS t')
        expect(result['data']).to eq([[1]])
        expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
      end

      context 'with JSONCompact format' do
        it 'returns formatted result' do
          result = Model.connection.do_execute('SELECT 1 AS t', format: 'JSONCompact')
          expect(result['data']).to eq([[1]])
          expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
        end
      end

      context 'with JSONCompactEachRowWithNamesAndTypes format' do
        it 'returns formatted result' do
          result = Model.connection.do_execute('SELECT 1 AS t', format: 'JSONCompactEachRowWithNamesAndTypes')
          expect(result['data']).to eq([[1]])
          expect(result['meta']).to eq([{ 'name' => 't', 'type' => 'UInt8' }])
        end
      end
    end

    describe '#create' do
      it 'creates a new record' do
        expect {
          Model.create!(
            event_name: 'some event',
            date: date
          )
        }.to change { Model.count }
      end

      it 'insert all' do
        if ActiveRecord::version >= Gem::Version.new('6')
          Model.insert_all([
            {event_name: 'some event 1', date: date},
            {event_name: 'some event 2', date: date},
          ])
          expect(Model.count).to eq(2)
        end
      end
    end

    describe '#update' do
      let!(:record) { Model.create!(event_name: 'some event', event_value: 1, date: date) }

      it 'update' do
        expect {
          Model.where(event_name: 'some event').update_all(event_value: 2)
        }.to_not raise_error
      end

      it 'update model with primary key' do
        expect {
          if IS_NEW_CLICKHOUSE_SERVER
            Model.first.update!(event_value: 2)
          else
            ModelPk.first.update!(event_value: 2)
          end
        }.to_not raise_error
      end
    end

    describe '#delete' do
      let!(:record) { Model.create!(event_name: 'some event', date: date) }

      it 'scope' do
        expect {
          Model.where(event_name: 'some event').delete_all
        }.to_not raise_error
      end

      it 'destroy model with primary key' do
        expect {
          if IS_NEW_CLICKHOUSE_SERVER
            Model.first.destroy!
          else
            ModelPk.first.destroy!
          end
        }.to_not raise_error
      end
    end

    describe '#find_by' do
      let!(:record) { Model.create!(event_name: 'some event', date: Date.current, datetime: Time.now) }

      it 'finds the record' do
        expect(Model.find_by(event_name: 'some event').attributes).to eq(record.attributes)
      end
    end

    describe '#reverse_order!' do
      it 'blank' do
        expect(Model.all.reverse_order!.map(&:event_name)).to eq([])
      end

      it 'select' do
        Model.create!(event_name: 'some event 1', date: 1.day.ago)
        Model.create!(event_name: 'some event 2', date: 2.day.ago)
        if IS_NEW_CLICKHOUSE_SERVER
          expect(Model.all.reverse_order!.to_sql).to eq('SELECT sample.* FROM sample ORDER BY sample.event_name DESC')
          expect(Model.all.reverse_order!.map(&:event_name)).to eq(['some event 2', 'some event 1'])
        else
          expect(Model.all.reverse_order!.to_sql).to eq('SELECT sample.* FROM sample ORDER BY sample.date DESC')
          expect(Model.all.reverse_order!.map(&:event_name)).to eq(['some event 1', 'some event 2'])
        end
      end
    end

    describe 'convert type with aggregations' do
      let!(:record1) { Model.create!(event_name: 'some event', event_value: 1, date: date) }
      let!(:record2) { Model.create!(event_name: 'some event', event_value: 3, date: date) }

      it 'integer' do
        expect(Model.select(Arel.sql('sum(event_value) AS event_value'))[0].event_value.class).to eq(Integer)
        expect(Model.select(Arel.sql('sum(event_value) AS value'))[0].attributes['value'].class).to eq(Integer)
        expect(Model.pluck(Arel.sql('sum(event_value)')).first[0].class).to eq(Integer)
      end
    end

    describe 'boolean column type' do
      let!(:record1) { Model.create!(event_name: 'some event', event_value: 1, date: date) }

      it 'bool result' do
        expect(Model.first.enabled.class).to eq(FalseClass)
      end

      it 'is mapped to :boolean' do
        type = Model.columns_hash['enabled'].type
        expect(type).to eq(:boolean)
      end
    end

    describe 'string column type as byte array' do
      let(:bytes) { (0..255).to_a }
      let!(:record1) { Model.create!(event_name: 'some event', byte_array: bytes.pack('C*')) }

      it 'keeps all bytes' do
        returned_byte_array = Model.first.byte_array

        expect(returned_byte_array.unpack('C*')).to eq(bytes)
      end
    end

    describe 'UUID column type' do
      let(:random_uuid) { SecureRandom.uuid }
      let!(:record1) do
        Model.create!(event_name: 'some event', event_value: 1, date: date, relation_uuid: random_uuid)
      end

      it 'is mapped to :uuid' do
        type = Model.columns_hash['relation_uuid'].type
        expect(type).to eq(:uuid)
      end

      it 'accepts proper value' do
        expect(record1.relation_uuid).to eq(random_uuid)
      end

      it 'accepts non-canonical uuid' do
        record1.relation_uuid = 'ABCD-0123-4567-89EF-dead-beef-0101-1010'
        expect(record1.relation_uuid).to eq('abcd0123-4567-89ef-dead-beef01011010')
      end

      it 'does not accept invalid values' do
        record1.relation_uuid = 'invalid-uuid'
        expect(record1.relation_uuid).to be_nil
      end
    end

    describe 'decimal column type' do
      let!(:record1) do
        Model.create!(event_name: 'some event', decimal_value: BigDecimal('95891.74'))
      end

      # If converted to float, the value would be 9589174.000000001. This happened previously
      # due to JSON parsing of numeric values to floats.
      it 'keeps precision' do
        decimal_value = Model.first.decimal_value
        expect(decimal_value).to eq(BigDecimal('95891.74'))
      end
    end

    describe '#settings' do
      it 'works' do
        sql = Model.settings(optimize_read_in_order: 1, cast_keep_nullable: 1).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS optimize_read_in_order = 1, cast_keep_nullable = 1')
      end

      it 'quotes' do
        sql = Model.settings(foo: :bar).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS foo = \'bar\'')
      end

      it 'allows passing the symbol :default to reset a setting' do
        sql = Model.settings(max_insert_block_size: :default).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample SETTINGS max_insert_block_size = DEFAULT')
      end
    end

    describe '#using' do
      it 'works' do
        sql = Model.joins(:joins).using(:event_name, :date).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample INNER JOIN joins USING event_name,date')
      end

      it 'works with filters' do
        sql = Model.joins(:joins).using(:event_name, :date).where(joins: { event_value: 1 }).to_sql
        expect(sql).to eq("SELECT sample.* FROM sample INNER JOIN joins USING event_name,date WHERE joins.event_value = 1")
      end
    end

    describe '#window' do
      it 'works' do
        sql = Model.window('x', order: 'date', partition: 'name', rows: 'UNBOUNDED PRECEDING').select('sum(event_value) OVER x').to_sql
        expect(sql).to eq('SELECT sum(event_value) OVER x FROM sample WINDOW x AS (PARTITION BY name ORDER BY date ROWS UNBOUNDED PRECEDING)')
      end

      it 'empty' do
        sql = Model.window('x').select('sum(event_value) OVER x').to_sql
        expect(sql).to eq('SELECT sum(event_value) OVER x FROM sample WINDOW x AS ()')
      end
    end

    describe 'arel predicates' do
      describe '#matches' do
        it 'uses ilike for case insensitive matches' do
          sql = Model.where(Model.arel_table[:event_name].matches('some event')).to_sql
          expect(sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name ILIKE 'some event'")
        end

        it 'uses like for case sensitive matches' do
          sql = Model.where(Model.arel_table[:event_name].matches('some event', nil, true)).to_sql
          expect(sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name LIKE 'some event'")
        end
      end
    end

    describe 'DateTime64 create' do
      it 'create a new record' do
        time = DateTime.parse('2023-07-21 08:00:00.123')
        Model.create!(datetime: time, datetime64: time)
        row = Model.first
        expect(row.datetime).to_not eq(row.datetime64)
        expect(row.datetime.strftime('%Y-%m-%d %H:%M:%S')).to eq('2023-07-21 08:00:00')
        expect(row.datetime64.strftime('%Y-%m-%d %H:%M:%S.%3N')).to eq('2023-07-21 08:00:00.123')
      end
    end

    describe 'final request' do
      let!(:record1) { Model.create!(date: date, event_name: '1') }
      let!(:record2) { Model.create!(date: date, event_name: '1') }

      it 'select' do
        expect(Model.count).to eq(2)
        expect(Model.final.count).to eq(1)
        expect(Model.final!.count).to eq(1)
        expect(Model.final.where(date: '2023-07-21').to_sql).to eq('SELECT sample.* FROM sample FINAL WHERE sample.date = \'2023-07-21\'')
      end
    end

    describe '#limit_by' do
      it 'works' do
        sql = Model.limit_by(1, :event_name).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample LIMIT 1 BY event_name')
      end

      it 'works with limit' do
        sql = Model.limit(1).limit_by(1, :event_name).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample LIMIT 1 BY event_name LIMIT 1')
      end
    end

    describe '#group_by_grouping_sets' do
      it 'raises an error with no arguments' do
        expect { Model.group_by_grouping_sets }.to raise_error(ArgumentError, 'The method .group_by_grouping_sets() must contain arguments.')
      end

      it 'works with the empty grouping set' do
        sql = Model.group_by_grouping_sets([]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( (  ) )')
      end

      it 'accepts strings' do
        sql = Model.group_by_grouping_sets(%w[foo bar], %w[baz]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( foo, bar ), ( baz ) )')
      end

      it 'accepts symbols' do
        sql = Model.group_by_grouping_sets(%i[foo bar], %i[baz]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( foo, bar ), ( baz ) )')
      end

      it 'accepts Arel nodes' do
        sql = Model.group_by_grouping_sets([Model.arel_table[:foo], Model.arel_table[:bar]], [Model.arel_table[:baz]]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( sample.foo, sample.bar ), ( sample.baz ) )')
      end

      it 'accepts mixed arguments' do
        sql = Model.group_by_grouping_sets(['foo', :bar], [Model.arel_table[:baz]]).to_sql
        expect(sql).to eq('SELECT sample.* FROM sample GROUP BY GROUPING SETS ( ( foo, bar ), ( sample.baz ) )')
      end
    end
  end

  context 'sample with id column' do
    class ModelWithoutPrimaryKey < ActiveRecord::Base
      self.table_name = 'sample_without_key'
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data_without_primary_key')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    it 'detect primary key' do
      expect(ModelWithoutPrimaryKey.primary_key).to eq(nil)
    end

    describe '#delete' do
      let!(:record) { ModelWithoutPrimaryKey.create!(event_name: 'some event', date: date) }

      it 'model destroy' do
        expect {
          record.destroy!
        }.to raise_error(ActiveRecord::ActiveRecordError, 'Deleting a row is not possible without a primary key')
      end

      it 'scope' do
        expect {
          ModelWithoutPrimaryKey.where(event_name: 'some event').delete_all
        }.to_not raise_error
      end
    end
  end

  context 'array' do
    let!(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'actions'
      end
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_array_datetime')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    describe '#create' do
      it 'creates a new record' do
        expect {
          model.create!(
            array_datetime: [1.day.ago, Time.now, '2022-12-06 15:22:49'],
            array_string: %w[asdf jkl],
            array_int: [1, 2],
            date: date
          )
        }.to change { model.count }
        event = model.first
        expect(event.array_datetime.is_a?(Array)).to be_truthy
        expect(event.array_datetime[0].is_a?(DateTime)).to be_truthy
        expect(event.array_string[0].is_a?(String)).to be_truthy
        expect(event.array_string).to eq(%w[asdf jkl])
        expect(event.array_int.is_a?(Array)).to be_truthy
        expect(event.array_int).to eq([1, 2])
      end

      it 'create with insert all' do
        expect {
          model.insert_all([{
            array_datetime: [1.day.ago, Time.now, '2022-12-06 15:22:49'],
            array_string: %w[asdf jkl],
            array_int: [1, 2],
            date: date
          }])
        }.to change { model.count }
      end

      it 'get record' do
        model.connection.insert("INSERT INTO #{model.table_name} (id, array_datetime, date) VALUES (1, '[''2022-12-06 15:22:49'',''2022-12-05 15:22:49'']', '2022-12-06')")
        expect(model.count).to eq(1)
        event = model.first
        expect(event.date.is_a?(Date)).to be_truthy
        expect(event.date).to eq(Date.parse('2022-12-06'))
        expect(event.array_datetime.is_a?(Array)).to be_truthy
        expect(event.array_datetime[0].is_a?(DateTime)).to be_truthy
        expect(event.array_datetime[0]).to eq('2022-12-06 15:22:49')
        expect(event.array_datetime[1]).to eq('2022-12-05 15:22:49')
      end
    end
  end

  context 'map' do
    let!(:model) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'verbs'
      end
    end

    before do
      migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_map_datetime')
      quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    end

    describe '#create' do
      it 'creates a new record' do
        expect {
          model.create!(
            map_datetime: {a: 1.day.ago, b: Time.now, c: '2022-12-06 15:22:49'},
            map_string: {a: 'asdf', b: 'jkl' },
            map_int: {a: 1, b: 2},
            map_array_datetime: {a: [1.day.ago], b: [Time.now, '2022-12-06 15:22:49']},
            map_array_string: {a: ['str'], b: ['str1', 'str2']},
            map_array_int: {a: [1], b: [1, 2, 3]},
            date: date
          )
        }.to change { model.count }.by(1)

        record = model.first
        expect(record.map_datetime).to be_a Hash
        expect(record.map_string).to be_a Hash
        expect(record.map_int).to be_a Hash
        expect(record.map_array_datetime).to be_a Hash
        expect(record.map_array_string).to be_a Hash
        expect(record.map_array_int).to be_a Hash

        expect(record.map_datetime['a']).to be_a DateTime
        expect(record.map_string['a']).to be_a String
        expect(record.map_string).to eq({'a' => 'asdf', 'b' => 'jkl'})
        expect(record.map_int).to eq({'a' => 1, 'b' => 2})

        expect(record.map_array_datetime['b']).to be_a Array
        expect(record.map_array_string['b']).to be_a Array
        expect(record.map_array_int['b']).to be_a Array
      end

      it 'create with insert all' do
        expect {
          model.insert_all([{
            map_datetime: {a: 1.day.ago, b: Time.now, c: '2022-12-06 15:22:49'},
            map_string: {a: 'asdf', b: 'jkl' },
            map_int: {a: 1, b: 2},
            map_array_datetime: {a: [1.day.ago], b: [Time.now, '2022-12-06 15:22:49']},
            map_array_string: {a: ['str'], b: ['str1', 'str2']},
            map_array_int: {a: [1], b: [1, 2, 3]},
            date: date
          }])
        }.to change { model.count }.by(1)
      end

      it 'get record' do
        model.connection.insert("INSERT INTO #{model.table_name} (id, map_datetime, map_array_datetime, date) VALUES (1, {'a': '2022-12-05 15:22:49', 'b': '2024-01-01 12:00:08'}, {'c': ['2022-12-05 15:22:49','2024-01-01 12:00:08']}, '2022-12-06')")
        expect(model.count).to eq(1)
        record = model.first
        expect(record.date.is_a?(Date)).to be_truthy
        expect(record.date).to eq(Date.parse('2022-12-06'))
        expect(record.map_datetime).to be_a Hash
        expect(record.map_datetime['a'].is_a?(DateTime)).to be_truthy
        expect(record.map_datetime['a']).to eq(DateTime.parse('2022-12-05 15:22:49'))
        expect(record.map_datetime['b']).to eq(DateTime.parse('2024-01-01 12:00:08'))
        expect(record.map_array_datetime).to be_a Hash
        expect(record.map_array_datetime['c']).to be_a Array
        expect(record.map_array_datetime['c'][0]).to eq(DateTime.parse('2022-12-05 15:22:49'))
        expect(record.map_array_datetime['c'][1]).to eq(DateTime.parse('2024-01-01 12:00:08'))
      end
    end
  end
end
