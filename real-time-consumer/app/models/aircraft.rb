class Aircraft
		
	attr_accessor :key, :latitude, :longitude, :speed,
		:model, :origin, :destination

	def initialize(key)
 	   self.key = key
	   hash = $redis.hgetall(key)
	   self.latitude = hash['latitude']
	   self.longitude = hash['longitude']
	   self.speed = hash['speed']
	   self.model = hash['aircraftModel']
	   self.origin = hash['origin']
           self.destination = hash['destination']
	  
	end
	
end
