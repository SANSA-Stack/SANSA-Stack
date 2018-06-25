package net.sansa_stack.ml.spark.outliers.vandalismdetection

import java.util.{ Arrays, List }
import java.util.{ HashSet, Set }

class UserFeatures extends Serializable {

  // User Is Privileged :
  def CheckName_isGlobalSysopUser(str: String): Boolean = {

    val GlobalSysopUser: Array[String] = Array("Alan", "BRUTE", "Defender", "Glaisher", "Igna", "Jafeluv", "Kaganer", "Liliana-60", "Mh7kJ", "MoiraMoira", "PiRSquared17", "Pmlineditor", "Stryn",
      "Tiptoety", "Toto Azéro", "Vogone", "Wim b")
    val users: Set[String] = new HashSet[String](Arrays.asList(GlobalSysopUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }
    result
  }

  def CheckName_isGlobalRollBackerUser(str: String): Boolean = {
    val isGlobalRollBackerUser: Array[String] =
      Array(".snoopy.", "Addihockey10",
        "Ah3kal", "Alan", "Aldnonymous", "Alison", "Avicennasis",
        "Az1568", "BRUTE", "Baiji", "Beetstra", "Church of emacs",
        "Courcelles", "Dalibor Bosits", "Defender", "Deu", "EdBever",
        "Erwin", "Erwin85", "Ezarate", "Fabexplosive", "FalconL",
        "Finnrind", "Gaeser", "Glaisher", "Hazard-SJ", "Hercule",
        "Holder", "Hu12", "Hydriz", "Igna", "Iluvatar", "Incnis Mrsi",
        "Iste Praetor", "Jafeluv", "Jamesofur", "Jasper Deng",
        "JenVan", "Jorunn", "Juliancolton", "Kanjy", "Krinkle", "Leyo",
        "LlamaAl", "Lukas²³", "Maximillion Pegasus", "Mercy", "Mh7kJ",
        "Mike.lifeguard", "MoiraMoira", "Morphypnos", "Nastoshka",
        "NuclearWarfare", "Petrb", "PiRSquared17", "Pmlineditor",
        "Reder", "Restu20", "Ruy Pugliesi", "Seewolf", "Stryn",
        "Syum90", "Techman224", "Tiptoety", "Toto Azéro", "VasilievVV",
        "Vogone", "Waihorace", "Werdan7", "Wiki13", "Xqt", "Ymblanter",
        "YourEyesOnly")

    val users: Set[String] = new HashSet[String](Arrays.asList(isGlobalRollBackerUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }
    result
  }

  def CheckName_isGlobalStewarUser(str: String): Boolean = {
    val GlobalStewarUser: Array[String] =
      Array("Ajraddatz", "Avraham", "Barras", "Bencmq",
        "Bennylin", "Billinghurst", "Bsadowski1", "DerHexer", "Elfix",
        "Hoo man", "J.delanoy", "Jyothis", "M7", "MBisanz",
        "MF-Warburg", "Mardetanha", "Matanya", "Mathonius", "Melos",
        "Mentifisto", "Pundit", "Quentinv57", "QuiteUnusual",
        "Rschen7754", "Ruslik0", "SPQRobin", "Savh", "Shanmugamp7",
        "Shizhao", "Snowolf", "Tegel", "Teles", "Trijnstel", "Vituzzu",
        "Wikitanvir")
    val users: Set[String] = new HashSet[String](Arrays.asList(GlobalStewarUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }

    result

  }

  def CheckName_isAdmin(str: String): Boolean = {

    val AdminNames: Array[String] =
      Array("-revi", "555", "Abuse filter", "Addshore",
        "Ajraddatz", "Alan ffm", "AmaryllisGardener", "Andre Engels",
        "Andreasmperu", "Arkanosis", "Ash Crow", "Ayack", "Bene*",
        "Benoit Rochon", "Bill william compton", "Calak", "Cheers!",
        "Chrumps", "Conny", "Courcelles", "Csigabi", "Delusion23",
        "Dexbot", "Ebraminio", "ElfjeTwaalfje", "Epìdosis", "FakirNL",
        "Florn88", "Fomafix", "Gabbe", "Hahc21", "Harmonia Amanda",
        "Hazard-SJ", "Hoo man", "Inkowik", "JAn Dudík", "Jakec",
        "Jasper Deng", "Jdforrester", "Jianhui67", "Jitrixis",
        "John F. Lewis", "Jon Harald Søby", "Ladsgroup", "LadyInGrey",
        "Legoktm", "Lymantria", "Matěj Suchánek", "Multichill",
        "Mushroom", "Nikosguard", "Nizil Shah", "Nouill", "PMG",
        "Pamputt", "Pasleim", "Penn Station", "Place Clichy",
        "Raymond", "Reaper35", "Ricordisamoa", "Rippitippi", "Romaine",
        "Rschen7754", "Rzuwig", "SPQRobin", "Saehrimnir", "Sannita",
        "Scott5114", "Sjoerddebruin", "Sk!d", "Snow Blizzard",
        "Sotiale", "Soulkeeper", "Steenth", "Stryn", "Sven Manguard",
        "TCN7JM", "Taketa", "Tobias1984", "Tpt", "ValterVB", "Vogone",
        "Vyom25", "Wagino 20100516", "Whym", "YMS", "Ymblanter",
        "Zolo", "분당선M", "콩가루")

    val users: Set[String] = new HashSet[String](Arrays.asList(AdminNames: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }

    result

  }
  def CheckName_isRollBackerUser(str: String): Boolean = {

    val RollBackerUser: Array[String] =
      Array("Abián", "Aconcagua", "Aftabuzzaman",
        "Amire80", "Andrew Gray", "Aude", "AutomaticStrikeout",
        "Avocato", "Base", "Bináris", "Bluemersen", "Brackenheim",
        "Byfserag", "Callanecc", "Cekli829", "ChongDae", "Closeapple",
        "DangSunAlt", "Danrok", "David1010", "Dereckson", "Deskana",
        "Dough4872", "Dusti", "Emaus", "Espeso", "Eurodyne", "FDMS4",
        "Faux", "Fredddie", "GZWDer", "GeorgeBarnick", "H.b.sh",
        "Haglaz", "Haplology", "Holger1959", "IXavier", "Indu",
        "It Is Me Here", "Jasper Deng", "Jayadevp13", "Jeblad",
        "JohnLewis", "Kevinhksouth", "Koavf", "KrBot", "Lakokat",
        "M4r51n", "MZMcBride", "Makecat", "Marek Mazurkiewicz",
        "Mateusz.ns", "Mediran", "Meisam", "Merlissimo", "Milad A380",
        "Morgankevinj", "NBS", "NahidSultan", "Namnguyenvn",
        "Natuur12", "Nirakka", "Osiris", "Palosirkka", "Petrb",
        "PinkAmpersand", "Powerek38", "Pratyya Ghosh",
        "PublicAmpersand", "Py4nf", "Pzoxicuvybtnrm", "Razr Nation",
        "Reach Out to the Truth", "Revi", "Rschen7754 public",
        "SHOTHA", "Silvonen", "Simeondahl", "Soap", "Steinsplitter",
        "Sumone10154", "TBrandley", "The Anonymouse", "The Herald",
        "The Rambling Man", "Tom Morris", "Totemkin", "Vacation9",
        "WTM", "Wnme", "Wylve", "XOXOXO", "Yair rand", "Yamaha5",
        "Ypnypn", "Zerabat", "Йо Асакура", "آرش", "درفش کاویانی",
        "فلورانس", "محمد عصام")

    val users: Set[String] = new HashSet[String](Arrays.asList(RollBackerUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }
    result
  }

  // isBot User :
  def CheckName_isLocalBotUser(str: String): Boolean = {
    val LocalBotUser: Array[String] =
      Array("AGbot", "AHbot", "Addbot", "AinaliBot",
        "AkkakkBot", "AlepfuBot", "Aplikasi-Bot", "AudeBot",
        "AvocatoBot", "AyackBot", "BaseBot", "BeneBot*", "BetaBot",
        "BinBot", "BotMultichill", "BotMultichillT", "Botapyb",
        "Botik", "BoulaurBot", "BrackiBot", "BraveBot", "Byrialbot",
        "CalakBot", "CennoBot", "Chembot", "Chobot", "Choboty",
        "Citing Bot", "Cyberbot I", "D2-bot", "DSisyphBot",
        "DangSunFlood", "DangSunBot", "DangSunFlood2", "DangSunBot2",
        "DanmicholoBot", "Dcirovicbot", "Dexbot", "DidymBot",
        "Dima st bk bot", "Dipsacus fullonum bot", "DixonDBot",
        "Docu with script", "Docu w. script", "Dom bot", "DrTrigonBot",
        "DynBot Srv2", "DynamicBot Srv2", "EdinBot", "EdwardsBot",
        "ElphiBot", "EmausBot", "Escabot", "Faebot", "FischBot",
        "Frettiebot", "FrigidBot", "GPUBot", "GZWDer (flood)",
        "GrammarwareBot", "Hawk-Eye-Bot", "HaxpettBot", "Hazard-Bot",
        "Hoo Bot", "Hurricanefan25 in the storm", "InductiveBot",
        "InfoRobBot", "InkoBot", "Innocent bot", "JackieBot",
        "JAnDbot", "JVbot", "JWbot", "JYBot", "JhsBot", "KLBot2",
        "Kompakt-bot", "KrBot", "KrattBot", "Krdbot", "L PBot",
        "Legobot", "Liangent-bot", "LinkRecoveryBot", "Louperibot",
        "MBAreaBot", "MagulBot", "MahdiBot", "Makecat-bot",
        "MalarzBOT", "MarmaseBot", "MatSuBot", "MatmaBot", "MedalBot",
        "MerlBot", "MerlIwBot", "Miguillen-bot", "MineoBot",
        "Nicolas1981Bot", "Nullzerobot", "OctraBot", "OrlodrimBot",
        "PBot", "PLbot", "Peter17-Bot", "Pigsonthewing-bot",
        "PoulpyBot", "ProteinBoxBot", "Ra-bot-nik", "ReimannBot",
        "Reinheitsgebot", "Revibot", "Rezabot", "RoboViolet",
        "RobotGMwikt", "RobotMichiel1972", "Ruud Koot (bot)", "SKbot",
        "SLiuBot", "SamoaBot", "SanniBot", "SbisoloBot", "sDrewthbot",
        "SDrewthbot", "ShinobiBot", "ShonagonBot", "Shuaib-bot",
        "Sk!dbot", "smbbot", "Smbbot", "SpBot", "StackerBot",
        "Steenthbot", "SuccuBot", "Svenbot", "Symac bot", "TambonBot",
        "The Anonybot", "ThieolBot", "TptBot",
        "Translation Notification Bot", "UnderlyingBot", "VIAFBot",
        "VIAFbot", "ValterVBot", "ViscoBot", "VollBot", "VsBot",
        "WYImporterBot", "Whymbot", "Widar of zolo", "YasBot",
        "ZaBOTka", "ZedlikBot")

    val users: Set[String] = new HashSet[String](Arrays.asList(LocalBotUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }

    result

  }

  def CheckName_isGlobalbotUser(str: String): Boolean = {

    val GlobalbotUser: Array[String] =
      Array("AHbot", "Addbot", "Aibot", "Alexbot",
        "AlleborgoBot", "AnankeBot", "ArthurBot", "AvicBot",
        "AvocatoBot", "BOT-Superzerocool", "BenzolBot",
        "BodhisattvaBot", "BotMultichill", "Broadbot", "ButkoBot",
        "CandalBot", "CarsracBot", "Chobot", "ChuispastonBot",
        "CocuBot", "D'ohBot", "DSisyphBot", "DarafshBot", "Dexbot",
        "Dinamik-bot", "DirlBot", "DixonDBot", "DragonBot", "Ebrambot",
        "EmausBot", "Escarbot", "FiriBot", "FoxBot", "Gerakibot",
        "GhalyBot", "GrouchoBot", "HRoestBot", "HerculeBot",
        "HydrizBot", "Invadibot", "JAnDbot", "JYBot", "JackieBot",
        "JhsBot", "Jotterbot", "Justincheng12345-bot", "KLBot2",
        "KamikazeBot", "LaaknorBot", "Louperibot", "Loveless",
        "Luckas-bot", "MSBOT", "MalafayaBot", "MastiBot", "MenoBot",
        "MerlIwBot", "Movses-bot", "MystBot", "NjardarBot",
        "Obersachsebot", "PixelBot", "Ptbotgourou", "RedBot",
        "Rezabot", "Ripchip Bot", "Robbot", "RobotQuistnix", "RoggBot",
        "Rubinbot", "SamoaBot", "SassoBot", "SieBot", "SilvonenBot",
        "Soulbot", "Synthebot", "Sz-iwbot", "TXiKiBoT", "Tanhabot",
        "Thijs!bot", "TinucherianBot II", "TjBot", "Ver-bot",
        "VolkovBot", "WarddrBOT", "WikitanvirBot", "Xqbot",
        "YFdyh-bot", "Zorrobot", "タチコマ robot")

    val users: Set[String] = new HashSet[String](Arrays.asList(GlobalbotUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }

    result

  }

  def CheckName_isExtensionBotUser(str: String): Boolean = {

    val ExtensionBotUser: Array[String] =
      Array("127.0.0.1", "Abuse filter", "Babel AutoCreate", "FuzzyBot", "MediaWiki message delivery", "Translation Notification Bot")

    val users: Set[String] = new HashSet[String](Arrays.asList(ExtensionBotUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }
    result
  }

  def CheckName_isRegisterUser(str: String): Boolean = {

    var result: Boolean = false
    if (str != "0000") {

      result = true
    }
    result
  }

  def CheckName_isBotUserWithoutBotFlagUser(str: String): Boolean = {
    val BotUserWithoutBotFlagUser: Array[String] =
      Array("1VeertjeBot", "AGbot", "Ajrbot",
        "AlepfuBot", "AxelBot", "BernsteinBot", "Bigbossrobot",
        "BonifazWolbot", "BotNinja", "Cheers!-bot", "Csvtodata",
        "CultureBot", "Danroks bot", "DarafshBot", "DbBot",
        "Descriptioncreator", "Deskanabot", "DidymBot", "DynBot Srv2",
        "DæghrefnBot", "Dɐ-Bot", "ElphiBot", "Faebot", "Fako85bot",
        "Fatemibot", "FischBot-test", "FloBot", "GanimalBot",
        "Gerakibot", "Global Economic Map Bot", "HaroldBot",
        "HaxpettBot", "Hurricanefan25 in the storm", "IDbot",
        "InductiveBot", "InfoRobBot", "JanitorBot", "JhealdBot",
        "JohlBot", "KamikazeBot", "Kartṛ-bot", "KrattBot1", "KRLS Bot",
        "talk:KRLS Bot", "KunMilanoRobot", "LinkRecoveryBot",
        "MarmaseBot", "MastiBot", "MedalBot", "MenoBot", "MergeBot",
        "MerlBot", "Mk-II", "MuBot", "Persian Wikis Janitor Bot",
        "Rodrigo Padula (BOT)", "SaschaBot", "talk:SKbot", "SPQRobot",
        "SteinsplitterBot", "Structor", "SvebertBot",
        "Theo's Little Bot", "ThetaBot", "US National Archives bot",
        "Vadbot", "VlsergeyBot", "Wakebrdkid's bot", "Whymbot",
        "Xaris333Bot", "talk:Xaris333Bot", "Xqbot", "Yuibot",
        "Zielmicha Bot", "ÖdokBot", "레비:봇")

    val users: Set[String] = new HashSet[String](Arrays.asList(BotUserWithoutBotFlagUser: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }
    result
  }

  def CheckName_isPropertyCreator(str: String): Boolean = {
    val PropertyCreator: Array[String] =
      Array("Danrok", "Emw", "Fralambert",
        "GZWDer", "Ivan A. Krestinin", "Joshbaumgartner", "Kolja21",
        "MichaelSchoenitzer", "Micru", "Nightwish62", "Paperoastro",
        "PinkAmpersand", "Superm401", "Viscontino")

    val users: Set[String] = new HashSet[String](Arrays.asList(PropertyCreator: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }
    result
  }
  def CheckName_isTranslator(str: String): Boolean = {

    val Translator: Array[String] =
      Array("Base", "Bene*", "Beta16", "Brackenheim",
        "Chrumps", "Ebraminio", "GZWDer", "GeorgeBarnick",
        "Giftzwerg 88", "Jasper Deng", "John F. Lewis", "Kaganer",
        "Matěj Suchánek", "Michgrig", "Pasleim", "Ricordisamoa",
        "Rschen7754", "Saehrimnir", "Sjoerddebruin", "Vogone", "Whym",
        "Yair rand", "Ата", "분당선M")
    val users: Set[String] = new HashSet[String](Arrays.asList(Translator: _*))

    var result: Boolean = false
    val input = str
    if (input != null) {
      var tmp: String = input.trim()
      tmp = input.toLowerCase()
      result = users.contains(tmp)
    }
    result
  }

  def IsRegisteroUser(str: String): Boolean = {
    var flag = false
    if (str != "NA") {
      flag = true
    }
    flag
  }

  def IsBirthDate(str: String): Boolean = {

    var flag = false
    if (str.contains("P569")) {
      flag = true
    }

    flag
  }
  def IsDeathDate(str: String): Boolean = {
    var flag = false
    if (str.contains("P570")) {
      flag = true
    }
    flag
  }
}
