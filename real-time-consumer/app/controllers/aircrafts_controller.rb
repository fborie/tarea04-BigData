class AircraftsController < ApplicationController
  def index
     keys = $redis.keys('*')
     aircrafts = []
     for key in keys
	aircraft = Aircraft.new(key)
	puts aircraft.to_json
	aircrafts << aircraft
     end
     @lastAircrafts = aircrafts.last(8)
     @aircraft = @lastAircrafts.first()
   end
end
