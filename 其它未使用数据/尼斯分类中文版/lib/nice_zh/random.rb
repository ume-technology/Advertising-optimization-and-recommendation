module NiceZh
  class Random
    def initialize
      @top_level = NICE_ZH_DATA.keys.sample
      @second_level = NICE_ZH_DATA[@top_level]['subs'].keys.sample
      @third_level = NICE_ZH_DATA[@top_level]['subs'][@second_level]['subs'].keys.sample
      @last_level = NICE_ZH_DATA[@top_level]['subs'][@second_level]['subs'][@third_level]['subs'].keys.sample
    end

    def take
      Product.new(
                  category: Category.new(levels: {top_level: @top_level, second_level: @second_level, third_level: @third_level, last_level: @last_level}),
                  name: NICE_ZH_DATA[@top_level]['subs'][@second_level]['subs'][@third_level]['subs'][@last_level]['name']
                )
    end
  end
end
