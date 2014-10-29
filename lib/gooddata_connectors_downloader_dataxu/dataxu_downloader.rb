module GoodData
  module Connectors
    module DownloaderDataxu

      class DataxuDownloader < Base::BaseDownloader

        FEED_MAPPING = {
            "entity_name" => 0,
            "entity_version" => 1,
            "field_name" => 2,
            "field_type" => 3,
            "field_order" => 4
        }

        MANIFEST_MAPPING = {
            "path" => 0,
            "date" => 2,
            "entity_name" => 3,
            "entity_version" => 4,
            "size" => 5,
            "hash" => 6
        }



        attr_accessor :client,:bulk_client

        def initialize(metadata,options = {})
          @type = "dataxu_downloader"
          $now = GoodData::Connectors::Metadata::Runtime.now
          super(metadata,options)
        end

        def define_mandatory_configuration
          {
              @type => ["key","secret","bucket","local_path","manifests","feeds"]
          }.merge!(super)
        end

        def define_default_configuration
          {
              @type => {}
          }
        end

        def define_default_entities
          []
        end

        def connect
          $log.info "Connecting to S3"
          AWS.config({
                         :access_key_id => @metadata.get_configuration_by_type_and_key(@type,"key"),
                         :secret_access_key => @metadata.get_configuration_by_type_and_key(@type,"secret")
                     })
          @s3 = AWS::S3.new
        end

        def load_entities_metadata
          bucket = @metadata.get_configuration_by_type_and_key(@type,"bucket")
          feed = @metadata.get_configuration_by_type_and_key(@type,"feed")
          local_path = @metadata.get_configuration_by_type_and_key(@type,"local_path")
          FileUtils.mkpath local_path if !Dir.exist?(local_path)
          File.open(local_path + "feed.csv", "w+") {|f| f.write(@s3.buckets[bucket].objects[feed].read) }
          source_csv = CSV.open(local_path + "feed.csv",:headers => false,:col_sep => "|")


          feed_tree = {}
          source_csv.each do |line|
            if (!feed_tree.include?(line[FEED_MAPPING["entity_name"]]))
              feed_tree[line[FEED_MAPPING["entity_name"]]] = {}
            end

            if (!feed_tree[line[FEED_MAPPING["entity_name"]]].include?(line[FEED_MAPPING["entity_version"]]))
              feed_tree[line[FEED_MAPPING["entity_name"]]][line[FEED_MAPPING["entity_version"]]] = []
            end

            feed_tree[line[FEED_MAPPING["entity_name"]]][line[FEED_MAPPING["entity_version"]]] << {
                "field_name" => line[FEED_MAPPING["field_name"]],
                "field_type" => line[FEED_MAPPING["field_type"]],
                "field_order" => line[FEED_MAPPING["field_order"]],
            }
          end
          #s3.buckets['my-bucket'].objects['key']

          feed_tree.values.each do |val|
            val.values.sort!{|x,y| x["field_order"] <=> y["field_order"]}
          end

          @metadata.list_entities.each do |entity|
            temporary_entity = Metadata::Entity.new({"id" => entity.id, "name" => entity.name})
            source_description = feed_tree[entity.id]["1.0"]
            if (!entity.disabled?)
              source_description.each do |field|
                type = nil
                case field["field_type"]
                  when "Varchar"
                    type = "string-255"
                  when "Integer","Numberic","Bigint"
                    type = "decimal-16-4"
                  when "Numberic"
                    type = "decimal-16-4"
                  when "Timestamp Without Time Zone"
                    type = "date-true"
                  else
                    $log.info "Unsupported salesforce type #{field["type"]} - using string(255) as default value"
                    type = "string-255"
                end
                field = Metadata::Field.new({
                    "id" => field["field_name"],
                    "name" => field["field_name"],
                    "type" => type,
                    "custom" => {"order" => field["field_order"]}
                })
                temporary_entity.add_field(field)
              end
              # Merging entity and disabling add of new fields
              entity.merge!(temporary_entity,entity.custom["load_fields_from_source_system"])
            end
          end


          manifests_path = @metadata.get_configuration_by_type_and_key(@type,"manifests")
          @manifests = []
          @s3.buckets[bucket].objects.with_prefix(manifests_path).each do |object|
            date_string = object.key.match(/[\d]*.[\d]*$/)[0]
            date = DateTime.strptime(date_string,"%Y%m%d.%H%M%S")
            @manifests << {"path" => object.key,"date" => date}
          end
          @manifests.sort!{|x,y| x["date"] <=> y["date"] }
        end


        def load_oldest_file_manifest(index = 0)
          local_path = @metadata.get_configuration_by_type_and_key(@type,"local_path")
          bucket = @metadata.get_configuration_by_type_and_key(@type,"bucket")
          manifest = @manifests[index]
          manifest_path = manifest["path"].split("/").last
          File.open(local_path + manifest_path, "w+") {|f| f.write(@s3.buckets[bucket].objects[manifest["path"]].read)}
          manifest_csv = CSV.open(local_path + manifest_path,:headers => false,:col_sep => "|")

          @metadata.list_entities.each do |entity|
            entity.runtime["file_manifests"] = []
          end

          manifest_csv.each do |manifest_row|
            entity = @metadata.get_entity(manifest_row[MANIFEST_MAPPING["entity_name"]])
            fail "There is entity in manifest, which is not in entity configuration file #{manifest_row[MANIFEST_MAPPING["entity_name"]]}" if entity.nil?
            entity.runtime["file_manifests"] << {
                "path" => manifest_row[MANIFEST_MAPPING["path"]],
                "date" => manifest_row[MANIFEST_MAPPING["date"]],
                "entity_version" => manifest_row[MANIFEST_MAPPING["entity_version"]],
                "size" => manifest_row[MANIFEST_MAPPING["size"]],
                "hash" => manifest_row[MANIFEST_MAPPING["hash"]]
            }
          end
        end


        def download_entity_data(metadata_entity)
          local_path = @metadata.get_configuration_by_type_and_key(@type,"local_path")
          bucket = @metadata.get_configuration_by_type_and_key(@type,"bucket")
          feeds = @metadata.get_configuration_by_type_and_key(@type,"feeds")
          manifests_to_download = metadata_entity.runtime["file_manifests"]
          FileUtils.mkpath("#{local_path}#{metadata_entity.id}/")

          metadata_entity.runtime["parsed_filenames"] = []

          manifests_to_download.each do |manifest|
            filename = manifest["path"].split("/").last
            File.open("#{local_path}#{metadata_entity.id}/#{filename}", 'wb') do |file|
              $log.info "Downloading entity #{metadata_entity.id} file - #{feeds}#{filename}"
              @s3.buckets[bucket].objects["#{feeds}#{filename}"].read do |chunk|
                file.write(chunk)
              end
              metadata_entity.runtime["parsed_filenames"] << "#{local_path}#{metadata_entity.id}/#{filename}"

              $log.info "Download finished"
            end
          end
          metadata_entity.custom["column_separator"] = "|"
          metadata_entity.custom["file_format"] = "gzip"
          metadata_entity.custom["skip_rows"] = 0
        end

        private


        def clean(metadata_entity)
          # Lets clean the runtime informations about the downloaded files
          metadata_entity.runtime.delete("synchronization_source_filename")
          metadata_entity.runtime.delete("source_deleted_filename")
          metadata_entity.runtime.delete("source_filename")
        end



      end

    end
  end
end