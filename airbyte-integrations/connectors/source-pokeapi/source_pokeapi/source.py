"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests

from base_python import AbstractSource, HttpStream, Stream

POKEMON_LIST = ['bulbasaur','ivysaur','venusaur','charmander','charmeleon','charizard','squirtle','wartortle','blastoise','caterpie','metapod','butterfree','weedle','kakuna','beedrill','pidgey','pidgeotto','pidgeot','rattata','raticate','spearow','fearow','ekans','arbok','pikachu','raichu','sandshrew','sandslash','nidoranf','nidorina','nidoqueen','nidoranm','nidorino','nidoking','clefairy','clefable','vulpix','ninetales','jigglypuff','wigglytuff','zubat','golbat','oddish','gloom','vileplume','paras','parasect','venonat','venomoth','diglett','dugtrio','meowth','persian','psyduck','golduck','mankey','primeape','growlithe','arcanine','poliwag','poliwhirl','poliwrath','abra','kadabra','alakazam','machop','machoke','machamp','bellsprout','weepinbell','victreebel','tentacool','tentacruel','geodude','graveler','golem','ponyta','rapidash','slowpoke','slowbro','magnemite','magneton','farfetchd','doduo','dodrio','seel','dewgong','grimer','muk','shellder','cloyster','gastly','haunter','gengar','onix','drowzee','hypno','krabby','kingler','voltorb','electrode','exeggcute','exeggutor','cubone','marowak','hitmonlee','hitmonchan','lickitung','koffing','weezing','rhyhorn','rhydon','chansey','tangela','kangaskhan','horsea','seadra','goldeen','seaking','staryu','starmie','mrmime','scyther','jynx','electabuzz','magmar','pinsir','tauros','magikarp','gyarados','lapras','ditto','eevee','vaporeon','jolteon','flareon','porygon','omanyte','omastar','kabuto','kabutops','aerodactyl','snorlax','articuno','zapdos','moltres','dratini','dragonair','dragonite','mewtwo','mew','chikorita','bayleef','meganium','cyndaquil','quilava','typhlosion','totodile','croconaw','feraligatr','sentret','furret','hoothoot','noctowl','ledyba','ledian','spinarak','ariados','crobat','chinchou','lanturn','pichu','cleffa','igglybuff','togepi','togetic','natu','xatu','mareep','flaaffy','ampharos','bellossom','marill','azumarill','sudowoodo','politoed','hoppip','skiploom','jumpluff','aipom','sunkern','sunflora','yanma','wooper','quagsire','espeon','umbreon','murkrow','slowking','misdreavus','unown','wobbuffet','girafarig','pineco','forretress','dunsparce','gligar','steelix','snubbull','granbull','qwilfish','scizor','shuckle','heracross','sneasel','teddiursa','ursaring','slugma','magcargo','swinub','piloswine','corsola','remoraid','octillery','delibird','mantine','skarmory','houndour','houndoom','kingdra','phanpy','donphan','porygon2','stantler','smeargle','tyrogue','hitmontop','smoochum','elekid','magby','miltank','blissey','raikou','entei','suicune','larvitar','pupitar','tyranitar','lugia','ho-oh','celebi','treecko','grovyle','sceptile','torchic','combusken','blaziken','mudkip','marshtomp','swampert','poochyena','mightyena','zigzagoon','linoone','wurmple','silcoon','beautifly','cascoon','dustox','lotad','lombre','ludicolo','seedot','nuzleaf','shiftry','taillow','swellow','wingull','pelipper','ralts','kirlia','gardevoir','surskit','masquerain','shroomish','breloom','slakoth','vigoroth','slaking','nincada','ninjask','shedinja','whismur','loudred','exploud','makuhita','hariyama','azurill','nosepass','skitty','delcatty','sableye','mawile','aron','lairon','aggron','meditite','medicham','electrike','manectric','plusle','minun','volbeat','illumise','roselia','gulpin','swalot','carvanha','sharpedo','wailmer','wailord','numel','camerupt','torkoal','spoink','grumpig','spinda','trapinch','vibrava','flygon','cacnea','cacturne','swablu','altaria','zangoose','seviper','lunatone','solrock','barboach','whiscash','corphish','crawdaunt','baltoy','claydol','lileep','cradily','anorith','armaldo','feebas','milotic','castform','kecleon','shuppet','banette','duskull','dusclops','tropius','chimecho','absol','wynaut','snorunt','glalie','spheal','sealeo','walrein','clamperl','huntail','gorebyss','relicanth','luvdisc','bagon','shelgon','salamence','beldum','metang','metagross','regirock','regice','registeel','latias','latios','kyogre','groudon','rayquaza','jirachi','deoxys','turtwig','grotle','torterra','chimchar','monferno','infernape','piplup','prinplup','empoleon','starly','staravia','staraptor','bidoof','bibarel','kricketot','kricketune','shinx','luxio','luxray','budew','roserade','cranidos','rampardos','shieldon','bastiodon','burmy','wormadam','mothim','combee','vespiquen','pachirisu','buizel','floatzel','cherubi','cherrim','shellos','gastrodon','ambipom','drifloon','drifblim','buneary','lopunny','mismagius','honchkrow','glameow','purugly','chingling','stunky','skuntank','bronzor','bronzong','bonsly','mimejr','happiny','chatot','spiritomb','gible','gabite','garchomp','munchlax','riolu','lucario','hippopotas','hippowdon','skorupi','drapion','croagunk','toxicroak','carnivine','finneon','lumineon','mantyke','snover','abomasnow','weavile','magnezone','lickilicky','rhyperior','tangrowth','electivire','magmortar','togekiss','yanmega','leafeon','glaceon','gliscor','mamoswine','porygon-z','gallade','probopass','dusknoir','froslass','rotom','uxie','mesprit','azelf','dialga','palkia','heatran','regigigas','giratina','cresselia','phione','manaphy','darkrai','shaymin','arceus','victini','snivy','servine','serperior','tepig','pignite','emboar','oshawott','dewott','samurott','patrat','watchog','lillipup','herdier','stoutland','purrloin','liepard','pansage','simisage','pansear','simisear','panpour','simipour','munna','musharna','pidove','tranquill','unfezant','blitzle','zebstrika','roggenrola','boldore','gigalith','woobat','swoobat','drilbur','excadrill','audino','timburr','gurdurr','conkeldurr','tympole','palpitoad','seismitoad','throh','sawk','sewaddle','swadloon','leavanny','venipede','whirlipede','scolipede','cottonee','whimsicott','petilil','lilligant','basculin','sandile','krokorok','krookodile','darumaka','darmanitan','maractus','dwebble','crustle','scraggy','scrafty','sigilyph','yamask','cofagrigus','tirtouga','carracosta','archen','archeops','trubbish','garbodor','zorua','zoroark','minccino','cinccino','gothita','gothorita','gothitelle','solosis','duosion','reuniclus','ducklett','swanna','vanillite','vanillish','vanilluxe','deerling','sawsbuck','emolga','karrablast','escavalier','foongus','amoonguss','frillish','jellicent','alomomola','joltik','galvantula','ferroseed','ferrothorn','klink','klang','klinklang','tynamo','eelektrik','eelektross','elgyem','beheeyem','litwick','lampent','chandelure','axew','fraxure','haxorus','cubchoo','beartic','cryogonal','shelmet','accelgor','stunfisk','mienfoo','mienshao','druddigon','golett','golurk','pawniard','bisharp','bouffalant','rufflet','braviary','vullaby','mandibuzz','heatmor','durant','deino','zweilous','hydreigon','larvesta','volcarona','cobalion','terrakion','virizion','tornadus','thundurus','reshiram','zekrom','landorus','kyurem','keldeo','meloetta','genesect','chespin','quilladin','chesnaught','fennekin','braixen','delphox','froakie','frogadier','greninja','bunnelby','diggersby','fletchling','fletchinder','talonflame','scatterbug','spewpa','vivillon','litleo','pyroar','flabebe','floette','florges','skiddo','gogoat','pancham','pangoro','furfrou','espurr','meowstic','honedge','doublade','aegislash','spritzee','aromatisse','swirlix','slurpuff','inkay','malamar','binacle','barbaracle','skrelp','dragalge','clauncher','clawitzer','helioptile','heliolisk','tyrunt','tyrantrum','amaura','aurorus','sylveon','hawlucha','dedenne','carbink','goomy','sliggoo','goodra','klefki','phantump','trevenant','pumpkaboo','gourgeist','bergmite','avalugg','noibat','noivern','xerneas','yveltal','zygarde','diancie','hoopa','volcanion','rowlet','dartrix','decidueye','litten','torracat','incineroar','popplio','brionne','primarina','pikipek','trumbeak','toucannon','yungoos','gumshoos','grubbin','charjabug','vikavolt','crabrawler','crabominable','oricorio','cutiefly','ribombee','rockruff','lycanroc','wishiwashi','mareanie','toxapex','mudbray','mudsdale','dewpider','araquanid','fomantis','lurantis','morelull','shiinotic','salandit','salazzle','stufful','bewear','bounsweet','steenee','tsareena','comfey','oranguru','passimian','wimpod','golisopod','sandygast','palossand','pyukumuku','typenull','silvally','minior','komala','turtonator','togedemaru','mimikyu','bruxish','drampa','dhelmise','jangmo-o','hakamo-o','kommo-o','tapukoko','tapulele','tapubulu','tapufini','cosmog','cosmoem','solgaleo','lunala','nihilego','buzzwole','pheromosa','xurkitree','celesteela','kartana','guzzlord','necrozma','magearna','marshadow','poipole','naganadel','stakataka','blacephalon','zeraora','meltan','melmetal','grookey','thwackey','rillaboom','scorbunny','raboot','cinderace','sobble','drizzile','inteleon','skwovet','greedent','rookidee','corvisquire','corviknight','blipbug','dottler','orbeetle','nickit','thievul','gossifleur','eldegoss','wooloo','dubwool','chewtle','drednaw','yamper','boltund','rolycoly','carkol','coalossal','applin','flapple','appletun','silicobra','sandaconda','cramorant','arrokuda','barraskewda','toxel','toxtricity','sizzlipede','centiskorch','clobbopus','grapploct','sinistea','polteageist','hatenna','hattrem','hatterene','impidimp','morgrem','grimmsnarl','obstagoon','perrserker','cursola','sirfetchd','mrrime','runerigus','milcery','alcremie','falinks','pincurchin','snom','frosmoth','stonjourner','eiscue','indeedee','morpeko','cufant','copperajah','dracozolt','arctozolt','dracovish','arctovish','duraludon','dreepy','drakloak','dragapult','zacian','zamazenta','eternatus','kubfu','urshifu','zarude','regieleki','regidrago','glastrier','spectrier','calyrex']

# Basic full refresh stream
class PokeapiStream(HttpStream):
    url_base = 'https://pokeapi.co/api/v2/'

    def __init__(self, name: str, **kwargs):
        super().__init__()
        self._name = name

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the base currency as a query param so we do that in this method
        return {"name": self.name}

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly,
        # so we just return a list containing the response
        return [response.json()]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination,
        # so we return None to indicate there are no more pages in the response
        return None


class Pokemon(PokeapiStream):
    def path(self, **kwargs) -> str:
        pokemon_name = self._name
        return f'pokemon/{pokemon_name}'

# Source
class SourcePokeapi(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        input_pokemon = config['name']
        if input_pokemon not in POKEMON_LIST:
            return False, f"Input Pokemon {input_pokemon} is invalid. Please check your spelling our input a valid Pokemon."
        else:
            return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        args = {"name": config["name"]}
        return [Pokemon(**args)]
