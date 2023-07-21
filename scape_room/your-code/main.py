import emoji
import time
from pygame import mixer
mixer.init()
mixer.set_num_channels(20)
mixer.set_reserved(1)

spaceship = '''  
 _                _            _     _   _           _   
| |    ___   ___ | | __   __ _| |_  | |_| |__   __ _| |_  
| |   / _ \ / _ \| |/ /  / _` | __| | __| '_ \ / _` | __|
| |__| (_) | (_) |   <  | (_| | |_  | |_| | | | (_| | |_ 
|_____\___/ \___/|_|\_\  \__,_|\__|  \__|_| |_|\__,_|\__|  
                                                         
     _        _                 _     _ _ 
    / \   ___| |_ ___ _ __ ___ | |   | | |
   / _ \ / __| __/ _ \ '__/ _ \| |/ _` | |
  / ___ \\__ \ || |__/ | | (_) | | (_| |_|
 /_/   \_\___/\__\___|_|  \___/|_|\__,_(_)
                                                    
 
                         ___
                     __,' __`.                _..----....____
         __...--.'``;.   ,.   ;``--..__     .'    ,-._    _.-'
   _..-''-------'   `'   `'   `'     O ``-''._   (,;') _,'
 ,'________________                          \`-._`-','
  `._              ```````````------...___   '-.._'-:
     ```--.._      ,.                     ````--...__\-.
             `.--. `-`                       ____    |  |`
               `. `.                       ,'`````.  ;  ;`
                 `._`.        __________   `.      \'__/`
                    `-:._____/______/___/____`.     \  `
                                |       `._    `.    \
                                `._________`-.   `.   `.___
                                                   `------'`
'''

space1 = '''

__   __                                               _ _ 
\ \ / /__  _   _    ___  ___  ___ __ _ _ __   ___  __| | |
 \ V / _ \| | | |  / _ \/ __|/ __/ _` | '_ \ / _ \/ _` | |
  | | (_) | |_| | |  __/\__ \ (_| (_| | |_) |  __/ (_| |_|
  |_|\___/ \__,_|  \___||___/\___\__,_| .__/ \___|\__,_(_)                                                                     
                                      |_|
                             ________
                            `---.     `.
                                 \      `.
                                  )_______`.    
                                .'        //`---...___
                               /         ||    // ||  `-._
<--------------- --------   )`-|   =//=  ||    ||  ||  / ).`.
<--------------- -------- ).-|         ||   || `........'`-.(o)
<--------------- -------- _...'.....__  ||   ||  _____ ||-\__.'
                        /_______..--'  ||   ||  -----  _  ||_/
                            `-------'      ||   ||   =\\=\_.'
<--------------- -------- )`-|         ||   ||       _..-'
<--------------- -------- ).-| =\\=    ||    \\  _.-' || 
                             ==\          \\   _.-'  /o'\
                                '-.__......_.-'      \__/
                                   \     .'
                                   /    /
                              ___.'    /
                              `-------'
'''


# In[5]:


#mixer.music.load('room1.mp3')
#mixer.music.set_volume(0.2)
#mixer.music.play(loops=-1)


# define rooms and items

#>>>>>>>>>>>>>>>>>>>>>>>>>> Furniture list>>>>>>>>>>>>>>>>>.
##Control Room Furniture list
commander_seat = {
    "name": "commander seat",
    "type": "furniture",
    "emoji": "\U0001F4BA"
}

instrument_panel = {
    "name": "instrument panel",
    "type": "furniture",
    "emoji": "\U0001F6E0"
}

##Engine Room Furniture list

cupola_window = {
    "name": "cupola window",
    "type": "furniture",
    "emoji": "\U0001FA9F"
}
zero_gravity_bed= {
    "name": "zero gravity bed",
    "type": "furniture",
    "emoji": "\U0001F6CF"
}
space_training_machine = {
    "name": "space training machine",
    "type": "furniture",
    "emoji": "\U0001F3CB"
}
closet = {
    "name": "closet",
    "type": "furniture",
    "emoji": "\U0001F5C3"
}
# Laboratory room Furniture list

cabinet = {
    "name": "cabinet",
    "type": "furniture",
    "emoji": "\U0001F5C4"
}
experiment_table = {
    "name": "experiment table",
    "type": "furniture",
    "emoji": "\U0001F9EA"
}
greenhouse = {
    "name": "greenhouse",
    "type": "furniture",
    "emoji": "\U0001FAB4"
}

# Doors List
alpha_door = { 
    "name": "alpha door",
    "type": "door",
    "sound":"door.mp3",
    "emoji": "\U0001F6AA"
}

beta_door = {
    "name": "beta door",
    "type": "door",
    "sound":"door.mp3",
    "emoji": "\U0001F6AA"
}

gamma_door = {
    "name": "gamma door",
    "type": "door",
    "sound":"door.mp3",
    "emoji": "\U0001F6AA"
}

space_door = {
    "name": "space door",
    "type": "door",
    "sound":"exit.mp3",
    "emoji": "\U0001F6AA"
}

## Docking room Furniture list

dirty_box = {
    "name": "dirty box",
    "type": "furniture",
    "emoji": "\U0001F4E6"
}

weird_bag = {
    "name": "weird bag",
    "type": "furniture",
    "emoji": "\U0001F602"
}

# Keys list
card_alpha = {
    "name": "access card alpha",
    "type": "key",
    "target": alpha_door,
    "sound":"card.mp3",
    "emoji": "\U0001F4B3"
}
card_beta = {
    "name": "access card beta",
    "type": "key",
    "target": beta_door,
    "sound":"card.mp3",
    "emoji": "\U0001F4B3"
}
card_gamma = {
    "name": "access card gamma",
    "type": "key",
    "target": gamma_door,
    "sound":"card.mp3",
    "emoji": "\U0001F4B3"
}
card_space = {
    "name": "space card",
    "type": "key",
    "target": space_door,
    "sound":"card.mp3",
    "emoji": "\U0001F4B3"
}


# Rooms list

control_room = {
    "name": "control room",
    "type": "room",
    "sound":"room1.mp3",
    "emoji": "\U0001F4BA"
}
engine_room = {
    "name": "engine room",
    "type": "room",
    "sound":"room2.mp3",
    "emoji": "\U0001F6E0"
}
laboratory = {
    "name": "laboratory",
    "type": "room",
    "sound":"room3.mp3",
    "emoji": "\U0001F52C"
}
docking_room = {
    "name": "docking room",
    "type": "room",
    "sound":"room4.mp3",
    "emoji": "\U0001F6F02"
}
space = {
    "name": "space",
    "type": "room",
    "sound":"exit.mp3",
    "emoji": ""
}
# Survival Items
galactic_gps = {
    "name": "galactic gps",
    "type": "survival_item",
    "target": "",
    "msg":" Without it I would get lost in the space fiuff!!",
    "sound":"gps.mp3",
    "emoji": "\U0001F9ED"
}
dog = {
    "name": "my dog Astro",
    "type": "survival_item",
    "target": "",
    "msg":" Uff, thank goodness I found you! We have to get out of the spaceship soon, let's go!!! ",
    "sound":"dog.mp3",
    "emoji": "\U0001F9AE"
}
picture = {
    "name": "my best friend Android-55FX",
    "type": "survival_item",
    "target": "",
    "msg":" It could have been a better photograph but there is no time to look any further.",
    "sound":"picture.mp3",
    "emoji": "\U0001F5BC"
}
fuel = {
    "name": "fuel",
    "type": "survival_item",
    "target": "",
    "msg":" It's not a lot of fuel, but it will at least help me escape.",
    "sound":"fuel.mp3",
    "emoji": "\U0001F6E2"
}
huggable_bear = {
    "name": "huggable bear",
    "type": "survival_item",
    "target": "",
    "msg":" I can't sleep without him, he can't stay on the spaceship.",
    "sound":"bear.mp3",
    "emoji": "\U0001F9F8"
}

lady_gaga_physical_album = {
    "name": "Lady Gaga's physical album",
    "type": "survival_item",
    "target": "",
    "msg":" Fortunately I found it, I will listen to it once I manage to escape!!! ",
    "sound":"ladygaga.mp3",
    "emoji": "\U0001F4BD"
}

## define text colours

class col:
  red='\033[31m'
  green='\033[32m'
  yellow='\033[33m'
  blue='\033[34m'
  purple='\033[35m'
  cyan='\033[36m'
  reset='\033[0m'

# all_rooms = [control_room, engine_room,laboratory,]

# all_doors = [door_a, door_b, door_c, door_d]

# define which items/rooms are related

object_relations = {
#room relations
    "control room": [ commander_seat, instrument_panel, alpha_door],
    "engine room": [cupola_window,  zero_gravity_bed, space_training_machine, closet,beta_door, gamma_door,alpha_door],
    "laboratory": [cabinet, experiment_table,greenhouse,beta_door],
    "docking room": [gamma_door, space_door, dirty_box, weird_bag],

#doors relations
    "alpha door": [control_room, engine_room],
    "beta door": [engine_room, laboratory],
    "space door": [docking_room,space],
    "gamma door": [engine_room, docking_room],
#Items relations
    "commander seat": [card_alpha],
    "closet": [picture],
    "cabinet": [fuel],
    "dirty box": [lady_gaga_physical_album],
    "instrument panel": [galactic_gps],
    "zero gravity bed": [dog],
    "space training machine": [card_beta],
    "greenhouse": [card_space],
    "experiment table": [card_gamma],
    "weird bag": [huggable_bear],
}

# define game state. Do not directly change this dict. 
# Instead, when a new game starts, make a copy of this
# dict and use the copy to store gameplay state. This 
# way you can replay the game multiple times.

INIT_GAME_STATE = {
    "current_room": control_room,
    "keys_collected": [],
    "items_collected": [],
    "target_room": space
}


# In[6]:


#### PLAYLIST NOT FINISHED

###import random

###songs = ("room1.mp3", "room2.mp3", "room3.mp3", "room4.mp3")
###mixer.music.set_volume(0.1)
###selected_song = random.choice(songs)   # select music file
###mixer.music.load(selected_song) # load music file
###mixer.music.play()              # play it


#mixer.music.play(loops=-1)           # Play the music



def green(x): print("\033[92m {}\033[00m" .format(x))
    
def linebreak():
    """
    Print a line break
    """
    print("\n\n")

def start_game():
    """
    Start the game
    """
    mixer.music.load('room1.mp3')
    mixer.music.set_volume(0.1)
    mixer.music.play(loops=-1)
    print("You are the captain of the spaceship Battlestar and the spaceship is about to collide with an asteroid and it is inevitable. You have to escape in the space shuttle and you have 2 minutes. Don't forget to take everything you need to escape!!!.")
    print(spaceship)
    play_room(game_state["current_room"]) ##### first step of the room

    ##### Starts in game_room (initialized in state)
    ##### value gets update when go_to_next_room functions gets called

def play_room(room):
    """
    Play a room. First check if the room being played is the target room.
    If it is, the game will end with success. Otherwise, let player either 
    explore (list all items in this room) or examine an item found here.
    """
    game_state["current_room"] = room ##### stores in a variable the room for better readibility 
    if(game_state["current_room"] == game_state["target_room"]): 
        print(col.red + "Congrats! You escaped from the spaceship!" + col.reset) ##### validates if you win or not
        print(space1)
        time.sleep(20)
        mixer.music.stop()
    else:
        print(f"You are now in {room['name']}")##### uses the name value of the game_room dict, this would be something like  game_state["current_room"]["name"]
        intended_action = input("What would you like to do? Type 'explore' , 'examine' , 'check inventory' or 'check map' ?").strip() 
        if intended_action == "explore":
            explore_room(room) ##### shows the items in the room depending on the current room
            play_room(room) ##### restart the process with print "You are now in room" 
        elif intended_action == "examine": 
            examine_item(input("What would you like to examine?").strip()) ##### type a value of the ones specified in explore function
        elif intended_action == "check inventory": 
            check_inventory() ##### type a value of the ones specified in explore function
            play_room(room)
        elif intended_action == "check map":
            print( "close map to continue" )
            import pygame
            pygame.init()
            w = 970;
            h = 555;
            screen = pygame.display.set_mode((w, h))
            pygame.display.set_caption('Spaceship Map')
            TPImage = pygame.image.load("spaceship.png").convert()
            # coordinates of the image
            x = 10;
            y = 20;
            screen.blit(TPImage, (x, y))
            # paint screen one time
            pygame.display.flip()
            running = True
            while (running): # loop listening for end of game
               for event in pygame.event.get():
                  if event.type == pygame.QUIT:
                     running = False
            # loop over, quite pygame
            pygame.display.quit()      
            play_room(room)
        else:
            print("Not sure what you mean. Type 'explore' , 'examine' , 'check inventory' or 'check map'.")
            play_room(room)
        linebreak()

def explore_room(room):
    """
    Explore a room. List all items belonging to this room.
    """
    items = [i["name"] for i in object_relations[room["name"]]] ##### defines a list of the items in current room (need to update object relations for each room)
    print("You explore the room. This is " + col.purple + room["name"] + col.reset + ". You find " + ", ".join(items))
    ##print("You explore the room. This is " + col.purple + room["name"] + col.reset + ". You find " + col.green + ", ".join(items) + col.reset)
    ##--> this breaks the item relations; 
    
def check_inventory():
    """
    Shows all the items that were collected.
    """
    if game_state['keys_collected']:
        print(f"The collected items & cards are: {[i['name'] for i in game_state['keys_collected']]}")
    else:
        print('You have not collected any items yet! Hurry up! The asteroid is not going to wait for you!')

def get_next_room_of_door(door, current_room):
    """
    From object_relations, find the two rooms connected to the given door.
    Return the room that is not the current_room.
    """
    connected_rooms = object_relations[door["name"]] ##### find relations of items to get the specific room
    for room in connected_rooms: 
        if(not current_room == room): ##### loops through the rooms to check if the current room is not the one that object relations points to and returned it 
            return room

def examine_item(item_name): ##### gets an input to examine
    """
    Examine an item which can be a door or furniture.
    First make sure the intended item belongs to the current room.
    Then check if the item is a door. Tell player if key hasn't been 
    collected yet. Otherwise ask player if they want to go to the next
    room. If the item is not a door, then check if it contains keys.
    Collect the key if found and update the game state. At the end,
    play either the current or the next room depending on the game state
    to keep playing.
    """
    current_room = game_state["current_room"] ##### defined as variable for future use in get_door_next_room
    next_room = ""
    output = None
    
    for item in object_relations[current_room["name"]]: ##### loop items list of current room values - for game_room would be couch, piano and door a
        if(item["name"] == item_name): ##### validates if input matches the object relations values 
            output = "You examine " + col.green + item_name + col.reset + item["emoji"] + ". "
            if(item["type"] == "door"): 
                have_key = False
                for key in game_state["keys_collected"]: ##### checks for keys of the dict values
                    if(key["target"] == item): ##### validates if that specific key opens that door (item)
                        have_key = True ##### turns the state to true if the key opens that door (item)
                if(have_key):
                    output += "You unlock it with an access card that you have" ##### appends the string value 
                    sound_effect = mixer.Sound(item["sound"])
                    sound_effect.set_volume(0.5)
                    sound_effect.play()
                    next_room = get_next_room_of_door(item, current_room) ##### item is a door - apply get_next_room_of_door function
                else:
                    output += "Upss, it is locked and you don't have the access card" ##### have_key remains false and blocks the process
            else: ##### this happens when the item is not a door
                if(item["name"] in object_relations and len(object_relations[item["name"]])>0):  ### when the item is at least one time present in object relations dict
                    item_found = object_relations[item["name"]].pop() ##### removes the key from the objet relations of the specific item, if all the keys are collected all the related items should be without value
                    game_state["keys_collected"].append(item_found) ##### adds the key to the collected state
                    output += "You find " + col.green + item_found["name"] + item_found["emoji"] + col.reset + "." ##### shows the key name
                    if "sound" in item_found:   
                        sound_effect = mixer.Sound(item_found["sound"])
                        sound_effect.set_volume(0.5)
                        sound_effect.play()                        
                    #print(f"The collected items & cards are: {[i['name'] for i in game_state['keys_collected']]}")
                    if "msg" in item_found: 
                        output += item_found["msg"]
                else:
                    output += "There isn't anything interesting about it." ##### item is selected and key is not present in there
            print(output)
            break 

    if(output is None):
        print("The item you requested is not found in the current room or your spelling is wrong.") ##### write an item that is not the one in the explore function
    
    if(next_room and input("Do you want to open the door and go through? Enter 'yes' or 'no'").strip() == 'yes'): ##### if next_room exists (defined after key is used when get_next_room fucntion is applied)
        play_room(next_room) ##### uses the target value of the item when the key is used 
        #print("You are now in...")
        #print(next_room["image"])
    else:
        play_room(current_room) ##### leaves you in the previous room if you say no

game_state = INIT_GAME_STATE.copy()

start_game() 





