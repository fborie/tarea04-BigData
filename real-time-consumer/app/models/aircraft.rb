class Aircraft
		
	attr_accessor :key, :latitude, :longitude, :speed,
		:model, :origin, :destiny

	def initialize(key)
 	   self.key = key
	   hash = $redis.hgetall(key)
	   self.latitude = hash['latitude']
	   self.longitude = hash['longitude']
	   self.speed = hash['speed']
	   self.model = hash['aircraftModel']
	   self.origin = hash['origin']
           self.destiny = hash['destiny']
	  
	end
	
end
