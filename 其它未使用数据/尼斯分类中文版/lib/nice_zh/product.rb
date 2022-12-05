module NiceZh
  class Product
    attr_reader :category, :name
    def initialize(category:, name:)
      @category = category
      @name = name
    end
  end
end
