# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 16:41:23 2024

@author: INVB05915
"""

class car (object):
    color = 'white'
    def __init__(self, max_speed, mila):
        self.max_speed = max_speed
        self.mila= mila
       
    
    def seating (self, seating):
        self.seating = seating
    
    def display_properties(self):
        
        print("Color",self.color)
        print("Maximum Speed",self.max_speed)
        print('Mileage',self.mila)
        print("Seating Capacity",self.seating)


veh1= car(200,50000)
veh2= car(180,75000)
veh1.seating (5)
veh2.seating(6)
veh1.display_properties()
veh2.display_properties()