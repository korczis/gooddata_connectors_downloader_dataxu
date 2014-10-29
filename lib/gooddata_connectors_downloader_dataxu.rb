require "gooddata_connectors_base"
require "gooddata"
require 'restforce'

require "gooddata_connectors_downloader_dataxu/version"
require "gooddata_connectors_downloader_dataxu/dataxu_downloader"

module GoodData
  module Connectors
    module DownloaderDataxu

      class DataxuDownloaderMiddleWare < GoodData::Bricks::Middleware

        def call(params)
          $log = params["GDC_LOGGER"]
          $log.info "Initializing DataxuDownloaderMiddleware"
          dataxu_downloader = DataxuDownloader.new(params["metadata_wrapper"],params)
          @app.call(params.merge('dataxu_downloader_wrapper' => dataxu_downloader))
        end



      end



    end
  end
end
