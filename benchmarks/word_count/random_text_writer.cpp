/*******************************************************************************
 * benchmarks/word_count/random_text_writer.cpp
 *
 * A C++ clone of org.apache.hadoop.examples.RandomTextWriter. The clone outputs
 * only text lines containing words. It uses the same words, but a different
 * underlying random generator.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>

#include <random>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

// see below for list of words
extern const std::vector<std::string> s_words;

std::string GenerateSentence(size_t num_words, std::mt19937& prng) {
    std::string sentence;
    for (size_t i = 0; i < num_words; ++i) {
        sentence += s_words[prng() % s_words.size()];
        // add space even at end, Java does this too.
        sentence += " ";
    }
    return sentence;
}

int main(int argc, char* argv[]) {

    common::CmdlineParser cp;

    unsigned min_words_key = 5, max_words_key = 10,
        min_words_value = 20, max_words_value = 100;

    unsigned seed = 123456;

    uint64_t totalbytes;

    cp.AddUInt('k', "min_words_key", "<N>", min_words_key,
                "minimum words in a key");
    cp.AddUInt('K', "max_words_key", "<N>", max_words_key,
                "maximum words in a key");

    cp.AddUInt('v', "min_words_value", "<N>", min_words_value,
                "minimum words in a value");
    cp.AddUInt('V', "max_words_value", "<N>", max_words_value,
                "maximum words in a value");

    cp.AddUInt('s', "seed", "<N>", seed,
                "random seed (default: 123456)");

    cp.AddParamBytes("totalbytes", totalbytes,
                      "total number of bytes to generate (approximately)");

    cp.SetVerboseProcess(false);
    if (!cp.Process(argc, argv)) {
        return -1;
    }

    cp.PrintResult(std::cerr);

    unsigned range_words_key = max_words_key - min_words_key;
    unsigned range_words_value = max_words_value - min_words_value;

    std::mt19937 prng(seed);

    uint64_t written_bytes = 0;

    while (written_bytes < totalbytes)
    {
        unsigned num_words_key = min_words_key + prng() % range_words_key;
        unsigned num_words_value = min_words_value + prng() % range_words_value;

        std::string key_words = GenerateSentence(num_words_key, prng);
        std::string value_words = GenerateSentence(num_words_value, prng);

        std::cout << key_words << value_words << '\n';

        written_bytes += key_words.size() + value_words.size() + 1;
    }

    return 0;
}

// list of words borrowed from
// hadoop-2.6.3-src/hadoop-mapreduce-project/hadoop-mapreduce-examples/
// src/main/java/org/apache/hadoop/examples/RandomTextWriter.java
const std::vector<std::string> s_words = {
    "diurnalness", "Homoiousian", "spiranthic", "tetragynian", "silverhead",
    "ungreat", "lithograph", "exploiter", "physiologian", "by", "hellbender",
    "Filipendula", "undeterring", "antiscolic", "pentagamist", "hypoid",
    "cacuminal", "sertularian", "schoolmasterism", "nonuple", "gallybeggar",
    "phytonic", "swearingly", "nebular", "Confervales", "thermochemically",
    "characinoid", "cocksuredom", "fallacious", "feasibleness", "debromination",
    "playfellowship", "tramplike", "testa", "participatingly", "unaccessible",
    "bromate", "experientialist", "roughcast", "docimastical", "choralcelo",
    "blightbird", "peptonate", "sombreroed", "unschematized",
    "antiabolitionist", "besagne", "mastication", "bromic", "sviatonosite",
    "cattimandoo", "metaphrastical", "endotheliomyoma", "hysterolysis",
    "unfulminated", "Hester", "oblongly", "blurredness", "authorling", "chasmy",
    "Scorpaenidae", "toxihaemia", "Dictograph", "Quakerishly", "deaf",
    "timbermonger", "strammel", "Thraupidae", "seditious", "plerome", "Arneb",
    "eristically", "serpentinic", "glaumrie", "socioromantic", "apocalypst",
    "tartrous", "Bassaris", "angiolymphoma", "horsefly", "kenno", "astronomize",
    "euphemious", "arsenide", "untongued", "parabolicness", "uvanite",
    "helpless", "gemmeous", "stormy", "templar", "erythrodextrin", "comism",
    "interfraternal", "preparative", "parastas", "frontoorbital", "Ophiosaurus",
    "diopside", "serosanguineous", "ununiformly", "karyological", "collegian",
    "allotropic", "depravity", "amylogenesis", "reformatory", "epidymides",
    "pleurotropous", "trillium", "dastardliness", "coadvice", "embryotic",
    "benthonic", "pomiferous", "figureheadship", "Megaluridae", "Harpa",
    "frenal", "commotion", "abthainry", "cobeliever", "manilla", "spiciferous",
    "nativeness", "obispo", "monilioid", "biopsic", "valvula", "enterostomy",
    "planosubulate", "pterostigma", "lifter", "triradiated", "venialness",
    "tum", "archistome", "tautness", "unswanlike", "antivenin",
    "Lentibulariaceae", "Triphora", "angiopathy", "anta", "Dawsonia", "becomma",
    "Yannigan", "winterproof", "antalgol", "harr", "underogating", "ineunt",
    "cornberry", "flippantness", "scyphostoma", "approbation", "Ghent",
    "Macraucheniidae", "scabbiness", "unanatomized", "photoelasticity",
    "eurythermal", "enation", "prepavement", "flushgate", "subsequentially",
    "Edo", "antihero", "Isokontae", "unforkedness", "porriginous", "daytime",
    "nonexecutive", "trisilicic", "morphiomania", "paranephros", "botchedly",
    "impugnation", "Dodecatheon", "obolus", "unburnt", "provedore",
    "Aktistetae", "superindifference", "Alethea", "Joachimite", "cyanophilous",
    "chorograph", "brooky", "figured", "periclitation", "quintette", "hondo",
    "ornithodelphous", "unefficient", "pondside", "bogydom", "laurinoxylon",
    "Shiah", "unharmed", "cartful", "noncrystallized", "abusiveness",
    "cromlech", "japanned", "rizzomed", "underskin", "adscendent", "allectory",
    "gelatinousness", "volcano", "uncompromisingly", "cubit", "idiotize",
    "unfurbelowed", "undinted", "magnetooptics", "Savitar", "diwata",
    "ramosopalmate", "Pishquow", "tomorn", "apopenptic", "Haversian",
    "Hysterocarpus", "ten", "outhue", "Bertat", "mechanist", "asparaginic",
    "velaric", "tonsure", "bubble", "Pyrales", "regardful", "glyphography",
    "calabazilla", "shellworker", "stradametrical", "havoc",
    "theologicopolitical", "sawdust", "diatomaceous", "jajman",
    "temporomastoid", "Serrifera", "Ochnaceae", "aspersor", "trailmaking",
    "Bishareen", "digitule", "octogynous", "epididymitis", "smokefarthings",
    "bacillite", "overcrown", "mangonism", "sirrah", "undecorated",
    "psychofugal", "bismuthiferous", "rechar", "Lemuridae", "frameable",
    "thiodiazole", "Scanic", "sportswomanship", "interruptedness", "admissory",
    "osteopaedion", "tingly", "tomorrowness", "ethnocracy", "trabecular",
    "vitally", "fossilism", "adz", "metopon", "prefatorial", "expiscate",
    "diathermacy", "chronist", "nigh", "generalizable", "hysterogen",
    "aurothiosulphuric", "whitlowwort", "downthrust", "Protestantize",
    "monander", "Itea", "chronographic", "silicize", "Dunlop", "eer",
    "componental", "spot", "pamphlet", "antineuritic", "paradisean",
    "interruptor", "debellator", "overcultured", "Florissant", "hyocholic",
    "pneumatotherapy", "tailoress", "rave", "unpeople", "Sebastian",
    "thermanesthesia", "Coniferae", "swacking", "posterishness", "ethmopalatal",
    "whittle", "analgize", "scabbardless", "naught", "symbiogenetically",
    "trip", "parodist", "columniform", "trunnel", "yawler", "goodwill",
    "pseudohalogen", "swangy", "cervisial", "mediateness", "genii",
    "imprescribable", "pony", "consumptional", "carposporangial", "poleax",
    "bestill", "subfebrile", "sapphiric", "arrowworm", "qualminess",
    "ultraobscure", "thorite", "Fouquieria", "Bermudian", "prescriber",
    "elemicin", "warlike", "semiangle", "rotular", "misthread", "returnability",
    "seraphism", "precostal", "quarried", "Babylonism", "sangaree", "seelful",
    "placatory", "pachydermous", "bozal", "galbulus", "spermaphyte",
    "cumbrousness", "pope", "signifier", "Endomycetaceae", "shallowish",
    "sequacity", "periarthritis", "bathysphere", "pentosuria", "Dadaism",
    "spookdom", "Consolamentum", "afterpressure", "mutter", "louse",
    "ovoviviparous", "corbel", "metastoma", "biventer", "Hydrangea", "hogmace",
    "seizing", "nonsuppressed", "oratorize", "uncarefully", "benzothiofuran",
    "penult", "balanocele", "macropterous", "dishpan", "marten", "absvolt",
    "jirble", "parmelioid", "airfreighter", "acocotl", "archesporial",
    "hypoplastral", "preoral", "quailberry", "cinque", "terrestrially",
    "stroking", "limpet", "moodishness", "canicule", "archididascalian",
    "pompiloid", "overstaid", "introducer", "Italical", "Christianopaganism",
    "prescriptible", "subofficer", "danseuse", "cloy", "saguran",
    "frictionlessly", "deindividualization", "Bulanda", "ventricous",
    "subfoliar", "basto", "scapuloradial", "suspend", "stiffish",
    "Sphenodontidae", "eternal", "verbid", "mammonish", "upcushion",
    "barkometer", "concretion", "preagitate", "incomprehensible", "tristich",
    "visceral", "hemimelus", "patroller", "stentorophonic", "pinulus",
    "kerykeion", "brutism", "monstership", "merciful", "overinstruct",
    "defensibly", "bettermost", "splenauxe", "Mormyrus", "unreprimanded",
    "taver", "ell", "proacquittal", "infestation", "overwoven", "Lincolnlike",
    "chacona", "Tamil", "classificational", "lebensraum", "reeveland",
    "intuition", "Whilkut", "focaloid", "Eleusinian", "micromembrane", "byroad",
    "nonrepetition", "bacterioblast", "brag", "ribaldrous", "phytoma",
    "counteralliance", "pelvimetry", "pelf", "relaster", "thermoresistant",
    "aneurism", "molossic", "euphonym", "upswell", "ladhood", "phallaceous",
    "inertly", "gunshop", "stereotypography", "laryngic", "refasten",
    "twinling", "oflete", "hepatorrhaphy", "electrotechnics", "cockal",
    "guitarist", "topsail", "Cimmerianism", "larklike", "Llandovery",
    "pyrocatechol", "immatchable", "chooser", "metrocratic", "craglike",
    "quadrennial", "nonpoisonous", "undercolored", "knob", "ultratense",
    "balladmonger", "slait", "sialadenitis", "bucketer", "magnificently",
    "unstipulated", "unscourged", "unsupercilious", "packsack", "pansophism",
    "soorkee", "percent", "subirrigate", "champer", "metapolitics",
    "spherulitic", "involatile", "metaphonical", "stachyuraceous",
    "speckedness", "bespin", "proboscidiform", "gul", "squit", "yeelaman",
    "peristeropode", "opacousness", "shibuichi", "retinize", "yote",
    "misexposition", "devilwise", "pumpkinification", "vinny", "bonze",
    "glossing", "decardinalize", "transcortical", "serphoid", "deepmost",
    "guanajuatite", "wemless", "arval", "lammy", "Effie", "Saponaria",
    "tetrahedral", "prolificy", "excerpt", "dunkadoo", "Spencerism",
    "insatiately", "Gilaki", "oratorship", "arduousness", "unbashfulness",
    "Pithecolobium", "unisexuality", "veterinarian", "detractive", "liquidity",
    "acidophile", "proauction", "sural", "totaquina", "Vichyite",
    "uninhabitedness", "allegedly", "Gothish", "manny", "Inger", "flutist",
    "ticktick", "Ludgatian", "homotransplant", "orthopedical", "diminutively",
    "monogoneutic", "Kenipsim", "sarcologist", "drome", "stronghearted",
    "Fameuse", "Swaziland", "alen", "chilblain", "beatable", "agglomeratic",
    "constitutor", "tendomucoid", "porencephalous", "arteriasis", "boser",
    "tantivy", "rede", "lineamental", "uncontradictableness", "homeotypical",
    "masa", "folious", "dosseret", "neurodegenerative", "subtransverse",
    "Chiasmodontidae", "palaeotheriodont", "unstressedly", "chalcites",
    "piquantness", "lampyrine", "Aplacentalia", "projecting", "elastivity",
    "isopelletierin", "bladderwort", "strander", "almud", "iniquitously",
    "theologal", "bugre", "chargeably", "imperceptivity", "meriquinoidal",
    "mesophyte", "divinator", "perfunctory", "counterappellant", "synovial",
    "charioteer", "crystallographical", "comprovincial", "infrastapedial",
    "pleasurehood", "inventurous", "ultrasystematic", "subangulated",
    "supraoesophageal", "Vaishnavism", "transude", "chrysochrous", "ungrave",
    "reconciliable", "uninterpleaded", "erlking", "wherefrom", "aprosopia",
    "antiadiaphorist", "metoxazine", "incalculable", "umbellic", "predebit",
    "foursquare", "unimmortal", "nonmanufacture", "slangy", "predisputant",
    "familist", "preaffiliate", "friarhood", "corelysis", "zoonitic", "halloo",
    "paunchy", "neuromimesis", "aconitine", "hackneyed", "unfeeble", "cubby",
    "autoschediastical", "naprapath", "lyrebird", "inexistency",
    "leucophoenicite", "ferrogoslarite", "reperuse", "uncombable", "tambo",
    "propodiale", "diplomatize", "Russifier", "clanned", "corona", "michigan",
    "nonutilitarian", "transcorporeal", "bought", "Cercosporella", "stapedius",
    "glandularly", "pictorially", "weism", "disilane", "rainproof", "Caphtor",
    "scrubbed", "oinomancy", "pseudoxanthine", "nonlustrous", "redesertion",
    "Oryzorictinae", "gala", "Mycogone", "reappreciate", "cyanoguanidine",
    "seeingness", "breadwinner", "noreast", "furacious", "epauliere",
    "omniscribent", "Passiflorales", "uninductive", "inductivity", "Orbitolina",
    "Semecarpus", "migrainoid", "steprelationship", "phlogisticate",
    "mesymnion", "sloped", "edificator", "beneficent", "culm",
    "paleornithology", "unurban", "throbless", "amplexifoliate",
    "sesquiquintile", "sapience", "astucious", "dithery", "boor", "ambitus",
    "scotching", "uloid", "uncompromisingness", "hoove", "waird", "marshiness",
    "Jerusalem", "mericarp", "unevoked", "benzoperoxide", "outguess", "pyxie",
    "hymnic", "euphemize", "mendacity", "erythremia", "rosaniline",
    "unchatteled", "lienteria", "Bushongo", "dialoguer", "unrepealably",
    "rivethead", "antideflation", "vinegarish", "manganosiderite",
    "doubtingness", "ovopyriform", "Cephalodiscus", "Muscicapa", "Animalivora",
    "angina", "planispheric", "ipomoein", "cuproiodargyrite", "sandbox",
    "scrat", "Munnopsidae", "shola", "pentafid", "overstudiousness", "times",
    "nonprofession", "appetible", "valvulotomy", "goladar", "uniarticular",
    "oxyterpene", "unlapsing", "omega", "trophonema", "seminonflammable",
    "circumzenithal", "starer", "depthwise", "liberatress", "unleavened",
    "unrevolting", "groundneedle", "topline", "wandoo", "umangite", "ordinant",
    "unachievable", "oversand", "snare", "avengeful", "unexplicit", "mustafina",
    "sonable", "rehabilitative", "eulogization", "papery", "technopsychology",
    "impressor", "cresylite", "entame", "transudatory", "scotale",
    "pachydermatoid", "imaginary", "yeat", "slipped", "stewardship", "adatom",
    "cockstone", "skyshine", "heavenful", "comparability", "exprobratory",
    "dermorhynchous", "parquet", "cretaceous", "vesperal", "raphis",
    "undangered", "Glecoma", "engrain", "counteractively", "Zuludom",
    "orchiocatabasis", "Auriculariales", "warriorwise", "extraorganismal",
    "overbuilt", "alveolite", "tetchy", "terrificness", "widdle",
    "unpremonished", "rebilling", "sequestrum", "equiconvex", "heliocentricism",
    "catabaptist", "okonite", "propheticism", "helminthagogic", "calycular",
    "giantly", "wingable", "golem", "unprovided", "commandingness", "greave",
    "haply", "doina", "depressingly", "subdentate", "impairment", "decidable",
    "neurotrophic", "unpredict", "bicorporeal", "pendulant", "flatman",
    "intrabred", "toplike", "Prosobranchiata", "farrantly", "toxoplasmosis",
    "gorilloid", "dipsomaniacal", "aquiline", "atlantite", "ascitic",
    "perculsive", "prospectiveness", "saponaceous", "centrifugalization",
    "dinical", "infravaginal", "beadroll", "affaite", "Helvidian",
    "tickleproof", "abstractionism", "enhedge", "outwealth", "overcontribute",
    "coldfinch", "gymnastic", "Pincian", "Munychian", "codisjunct", "quad",
    "coracomandibular", "phoenicochroite", "amender", "selectivity", "putative",
    "semantician", "lophotrichic", "Spatangoidea", "saccharogenic", "inferent",
    "Triconodonta", "arrendation", "sheepskin", "taurocolla", "bunghole",
    "Machiavel", "triakistetrahedral", "dehairer", "prezygapophysial",
    "cylindric", "pneumonalgia", "sleigher", "emir", "Socraticism", "licitness",
    "massedly", "instructiveness", "sturdied", "redecrease", "starosta",
    "evictor", "orgiastic", "squdge", "meloplasty", "Tsonecan",
    "repealableness", "swoony", "myesthesia", "molecule", "autobiographist",
    "reciprocation", "refective", "unobservantness", "tricae", "ungouged",
    "floatability", "Mesua", "fetlocked", "chordacentrum", "sedentariness",
    "various", "laubanite", "nectopod", "zenick", "sequentially", "analgic",
    "biodynamics", "posttraumatic", "nummi", "pyroacetic", "bot", "redescend",
    "dispermy", "undiffusive", "circular", "trillion", "Uraniidae", "ploration",
    "discipular", "potentness", "sud", "Hu", "Eryon", "plugger", "subdrainage",
    "jharal", "abscission", "supermarket", "countergabion", "glacierist",
    "lithotresis", "minniebush", "zanyism", "eucalypteol", "sterilely",
    "unrealize", "unpatched", "hypochondriacism", "critically", "cheesecutter"
};

/******************************************************************************/
