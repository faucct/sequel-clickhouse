RSpec.describe Sequel::Clickhouse do
  it 'has a version number' do
    expect(Sequel::Clickhouse::VERSION).not_to be nil
  end

  describe described_class::Database do
    describe '#create_table' do
      subject do
        lambda do
          connection.create_table :schema_migrations, engine: 'MergeTree(filename)' do
            String :filename, primary_key: true
          end
          connection.schema(:schema_migrations)
        end
      end
      let(:connection) { Sequel.connect(adapter: 'clickhouse', database: 'stat') }

      it { is_expected.not_to raise_error }
    end
  end
end
