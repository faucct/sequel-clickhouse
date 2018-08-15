require 'sequel'
require 'clickhouse'

require 'sequel/clickhouse/version'

module Sequel
  # Sequel adapter for the ClickHouse
  # @see https://github.com/jeremyevans/sequel
  # @see https://clickhouse.yandex
  module Clickhouse
    class Database < Sequel::Database # rubocop:disable
      set_adapter_scheme :clickhouse

      def execute(sql, **options)
        synchronize(options[:server]) { |conn| conn.execute sql }
      rescue ::Clickhouse::QueryError
        raise DatabaseError
      end

      def query(sql, **options)
        synchronize(options[:server]) { |conn| conn.query sql }
      rescue ::Clickhouse::QueryError
        raise DatabaseError
      end

      def tables(**options)
        synchronize(options[:server], &:tables)
      rescue ::Clickhouse::QueryError
        raise DatabaseError
      end

      def connect(server)
        ::Clickhouse.connect(server_opts(server))
      end

      def dataset_class_default
        Dataset
      end

      def column_definition_primary_key_sql(*)
      end

      def column_definition_null_sql(*)
      end

      def create_table_sql(name, generator, options)
        primary_key_columns =
          generator.columns.select { |c| c[:primary_key] }.map { |column| quote_identifier(column[:name]) }
        if primary_key_columns.empty?
          super
        else
          super + " ENGINE = ReplacingMergeTree ORDER BY (#{primary_key_columns.join(',')})"
        end
      end

      def type_literal_generic_string(column)
        raise column.inspect if column.key?(:null)
        if column[:text]
          'String'
        else
          "FixedString(#{column[:size] || default_string_column_size})"
        end
      end

      def identifier_input_method_default
        nil
      end

      def schema_parse_table(table, dataset: nil, server: nil, **)
        input_identifier_meth = input_identifier_meth(dataset)
        result = synchronize(server) do |conn|
          conn.query("DESCRIBE TABLE #{input_identifier_meth.call(table)}")
        end
        output_identifier_meth = output_identifier_meth(dataset)
        result.map do |row|
          attributes = result.names.zip(row).to_h
          [output_identifier_meth.call(attributes['name']), type: attributes['type']]
        end
      end

      def schema_column_type(type)
        case type
        when /^Nullable\((.+)\)$/ then
          schema_column_type(Regexp.last_match(1))
        else
          super
        end
      end
    end

    class Dataset < Sequel::Dataset
      def fetch_rows(sql)
        result_set = db.query(sql)
        names = result_set.names.map(&:to_sym)
        result_set.each { |row| yield names.zip(row).to_h }
      end

      def select_from_sql(sql)
        super
        sql << ' FINAL'
      end
    end
  end
end
