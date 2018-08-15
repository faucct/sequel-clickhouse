RSpec.describe Sequel::Clickhouse do
  it 'has a version number' do
    expect(Sequel::Clickhouse::VERSION).not_to be nil
  end
  let(:connection) { Sequel.connect(adapter: 'clickhouse', database: 'stat') }

  describe described_class::Database do
    describe '#create_table' do
      subject do
        lambda do
          connection.drop_table :schema_migrations
          connection.create_table :schema_migrations, engine: 'MergeTree(filename)' do
            String :filename, primary_key: true
          end
          connection.schema(:schema_migrations)
        end
      end

      it { is_expected.not_to raise_error }
    end
  end

  describe Sequel::Clickhouse::Dataset do
    describe '#fetch_rows' do
      let(:model) do
        Class.new(Sequel::Model(connection[:rosing_channels])) do
          self.unrestrict_primary_key
          self.use_transactions = false
          set_primary_key :uid
          plugin :dirty
        end
      end

      it do
        model.new(uid: '123', name: 'BBC').save
      end
    end
  end
end
