# frozen_string_literal: true

module ClickhouseActiverecord
  class Settings < ::ActiveSupport::CurrentAttributes
    # This is a thread-local global store. See ActiveSupport::CurrentAttributes for more info.
    attribute :wait_for_async_insert
    attribute :async_insert

    def insert_settings
      { wait_for_async_insert: wait_for_async_insert || 0, async_insert: async_insert || 1 }
    end
  end
end
