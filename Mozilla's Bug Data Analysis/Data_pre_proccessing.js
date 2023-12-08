// Data pre-proccessing
use FCP

const columnsWithHighPercentage = [];

// Step 1: Find columns with "---" values
const findColumnsQuery = [
  {
    $project: {
      _id: 0,
      columns: { $objectToArray: "$$ROOT" } // converts documents into an array of key-value pairs
    }
  },
  {
    $unwind: "$columns" // creates a separate document for each array element
  },
  {
    $match: { // filter documents where the value is "---"
      "columns.v": {
        $eq: "---"
      },
      "columns.v": {
        $type: "string"
      }
    }
  },
  {
    $group: {
      _id: "$columns.k" // bring a list of columns that contain "---" as one of their values
    }
  }
];

// save the resulting list of columns to iterate over it
const columnsWithDash = db.Bugs.aggregate(findColumnsQuery).toArray();

// Step 2: Calculate percentage of "---" value for each column
// identify those with >= 99% instances of "---" in a column
for (const columnObj of columnsWithDash) {
  const column = columnObj._id;

  const countQuery = {
    [column]: { $eq: "---" }
  };

  const columnCountQuery = [ // aggregation pipeline to count the total values in a field
    {
      $match: {
        [column]: { $exists: true } // filter documents based on the existance of a column
      }
    },
    {
      $group: {
        _id: null,
        count: { $sum: 1 } // count the total number of values in this column
      }
    }
  ];

  const columnCountResult = db.Bugs.aggregate(columnCountQuery).toArray(); // store the result of the aggregation query as an array
  const totalValues = columnCountResult.length > 0 ? columnCountResult[0].count : 0; // take into account cases where there are no documents

  const count = db.Bugs.countDocuments(countQuery); // count the number of "---" instances

  const percentage = (count / totalValues) * 100; // calculate the percentage of "---" in the column

  if (percentage >= 99) {
    columnsWithHighPercentage.push({ column, percentage });
  }
}

// Print columns with high percentage
for (const columnObj of columnsWithHighPercentage) {
  const { column, percentage } = columnObj;
  print(`Column: ${column}`);
  print(`Percentage of "---" values: ${percentage.toFixed(2)}%\n`);
}


// Splitting collections

// references
db.createCollection('References_v2'); 
	
var cursor = db.Bugs.find({}, {id: 1, blocks: 1, depends_on: 1, url: 1, see_also: 1, priority: 1, severity: 1, regressed_by: 1, regressions: 1});

while (cursor.hasNext()) {
  var document = cursor.next();
  db.References_v2.insertOne(document);
}

// extra bug detail
db.createCollection('Extra_bug_detail'); 
	
var cursor = db.Bugs.find({}, {id: 1, keywords: 1, whiteboard: 1,  cf_qa_whiteboard: 1,  votes: 1,  cf_has_str: 1,  flags: 1, alias: 1});

while (cursor.hasNext()) {
  var document = cursor.next();
  db.Extra_bug_detail.insertOne(document);
}

// people
db.createCollection("people"); 	

var cursor = db.Bugs.find({}, { assigned_to: 1, assigned_to_detail: 1, 
    creator: 1, creator_detail: 1, cc: 1, cc_detail: 1, qa_contact: 1, 
    qa_contact_detail: 1, mentors: 1, mentors_detail: 1, id:1});

while (cursor.hasNext()) {
  var document = cursor.next();
  db.people.insertOne(document);
}

// main bug summary
db.createCollection("main_bug_summary"); 	

var cursor = db.Bugs.find({}, { summary: 1, product: 1, 
    component: 1, priority: 1, cf_fx_points: 1, version: 1, 
    id: 1, type: 1, platform: 1, severity: 1, 
    op_sys: 1, classification: 1});

while (cursor.hasNext()) {
  var document = cursor.next();
  db.main_bug_summary.insertOne(document);
}

// tracking
db.createCollection("Tracking"); 	

var cursor = db.proj.find({}, 
{'id': 1,
 '_id': 0,
 'status': 1,
 'resolution': 1,
 'dupe_of': 1,
 'duplicates': 1,
 'target_milestone': 1,
 'cf_fx_iteration': 1,
 'cf_accessibility_severity': 1,
 'cf_a11y_review_project_flag': 1,
 'cf_webcompat_priority': 1,
 'cf_performance_impact': 1,
 'cf_status_191': 1,
 'cf_status_192': 1,
 'cf_status_20': 1,
 'cf_status_b2g_1_4': 1,
 'cf_status_b2g_2_0': 1,
 'cf_status_b2g_2_1': 1,
 'cf_status_b2g_2_1_s': 1,
 'cf_status_esr10': 1,
 'cf_status_firefox100': 1,
 'cf_status_firefox101': 1,
 'cf_status_firefox107': 1,
 'cf_status_firefox110': 1,
 'cf_status_firefox111': 1,
 'cf_status_firefox113': 1,
 'cf_status_firefox114': 1,
 'cf_status_firefox115': 1,
 'cf_status_firefox116': 1,
 'cf_status_firefox13': 1,
 'cf_status_firefox16': 1,
 'cf_status_firefox17': 1,
 'cf_status_firefox18': 1,
 'cf_status_firefox20': 1,
 'cf_status_firefox21': 1,
 'cf_status_firefox22': 1,
 'cf_status_firefox23': 1,
 'cf_status_firefox24': 1,
 'cf_status_firefox28': 1,
 'cf_status_firefox29': 1,
 'cf_status_firefox30': 1,
 'cf_status_firefox31': 1,
 'cf_status_firefox35': 1,
 'cf_status_firefox38': 1,
 'cf_status_firefox39': 1,
 'cf_status_firefox40': 1,
 'cf_status_firefox41': 1,
 'cf_status_firefox42': 1,
 'cf_status_firefox43': 1,
 'cf_status_firefox44': 1,
 'cf_status_firefox45': 1,
 'cf_status_firefox47': 1,
 'cf_status_firefox48': 1,
 'cf_status_firefox49': 1,
 'cf_status_firefox51': 1,
 'cf_status_firefox52': 1,
 'cf_status_firefox53': 1,
 'cf_status_firefox54': 1,
 'cf_status_firefox55': 1,
 'cf_status_firefox56': 1,
 'cf_status_firefox57': 1,
 'cf_status_firefox58': 1,
 'cf_status_firefox59': 1,
 'cf_status_firefox6': 1,
 'cf_status_firefox60': 1,
 'cf_status_firefox61': 1,
 'cf_status_firefox62': 1,
 'cf_status_firefox63': 1,
 'cf_status_firefox64': 1,
 'cf_status_firefox65': 1,
 'cf_status_firefox66': 1,
 'cf_status_firefox68': 1,
 'cf_status_firefox69': 1,
 'cf_status_firefox70': 1,
 'cf_status_firefox71': 1,
 'cf_status_firefox73': 1,
 'cf_status_firefox76': 1,
 'cf_status_firefox77': 1,
 'cf_status_firefox78': 1,
 'cf_status_firefox83': 1,
 'cf_status_firefox85': 1,
 'cf_status_firefox86': 1,
 'cf_status_firefox87': 1,
 'cf_status_firefox88': 1,
 'cf_status_firefox89': 1,
 'cf_status_firefox90': 1,
 'cf_status_firefox91': 1,
 'cf_status_firefox94': 1,
 'cf_status_firefox95': 1,
 'cf_status_firefox96': 1,
 'cf_status_firefox_esr102': 1,
 'cf_status_firefox_esr115': 1,
 'cf_status_firefox_esr31': 1,
 'cf_status_firefox_esr45': 1,
 'cf_status_firefox_esr52': 1,
 'cf_status_firefox_esr60': 1,
 'cf_status_firefox_esr78': 1,
 'cf_status_firefox_esr91': 1,
 'cf_status_seamonkey21': 1,
 'cf_status_seamonkey210': 1,
 'cf_status_seamonkey212': 1,
 'cf_status_seamonkey214': 1,
 'cf_status_seamonkey215': 1,
 'cf_status_seamonkey216': 1,
 'cf_status_seamonkey226': 1,
 'cf_status_seamonkey227': 1,
 'cf_status_seamonkey23': 1,
 'cf_status_seamonkey232': 1,
 'cf_status_seamonkey235': 1,
 'cf_status_seamonkey239': 1,
 'cf_status_seamonkey24': 1,
 'cf_status_seamonkey240': 1,
 'cf_status_seamonkey241': 1,
 'cf_status_seamonkey242': 1,
 'cf_status_seamonkey243': 1,
 'cf_status_seamonkey249': 1,
 'cf_status_seamonkey25': 1,
 'cf_status_seamonkey253': 1,
 'cf_status_seamonkey257esr': 1,
 'cf_status_seamonkey26': 1,
 'cf_status_seamonkey263': 1,
 'cf_status_thunderbird31': 1,
 'cf_status_thunderbird_106': 1,
 'cf_status_thunderbird_113': 1,
 'cf_status_thunderbird_114': 1,
 'cf_status_thunderbird_115': 1,
 'cf_status_thunderbird_116': 1,
 'cf_status_thunderbird_34': 1,
 'cf_status_thunderbird_36': 1,
 'cf_status_thunderbird_38': 1,
 'cf_status_thunderbird_39': 1,
 'cf_status_thunderbird_40': 1,
 'cf_status_thunderbird_41': 1,
 'cf_status_thunderbird_42': 1,
 'cf_status_thunderbird_44': 1,
 'cf_status_thunderbird_45': 1,
 'cf_status_thunderbird_46': 1,
 'cf_status_thunderbird_52': 1,
 'cf_status_thunderbird_53': 1,
 'cf_status_thunderbird_57': 1,
 'cf_status_thunderbird_58': 1,
 'cf_status_thunderbird_60': 1,
 'cf_status_thunderbird_61': 1,
 'cf_status_thunderbird_63': 1,
 'cf_status_thunderbird_64': 1,
 'cf_status_thunderbird_65': 1,
 'cf_status_thunderbird_83': 1,
 'cf_status_thunderbird_84': 1,
 'cf_status_thunderbird_85': 1,
 'cf_status_thunderbird_87': 1,
 'cf_status_thunderbird_91': 1,
 'cf_status_thunderbird_92': 1,
 'cf_status_thunderbird_93': 1,
 'cf_status_thunderbird_esr102': 1,
 'cf_status_thunderbird_esr115': 1,
 'cf_status_thunderbird_esr38': 1,
 'cf_status_thunderbird_esr60': 1,
 'cf_status_thunderbird_esr78': 1,
 'cf_status_thunderbird_esr91': 1,
 'cf_tracking_b2g': 1,
 'cf_tracking_b2g18': 1,
 'cf_tracking_bmo_push': 1,
 'cf_tracking_firefox10': 1,
 'cf_tracking_firefox11': 1,
 'cf_tracking_firefox113': 1,
 'cf_tracking_firefox114': 1,
 'cf_tracking_firefox115': 1,
 'cf_tracking_firefox116': 1,
 'cf_tracking_firefox13': 1,
 'cf_tracking_firefox22': 1,
 'cf_tracking_firefox23': 1,
 'cf_tracking_firefox25': 1,
 'cf_tracking_firefox27': 1,
 'cf_tracking_firefox30': 1,
 'cf_tracking_firefox31': 1,
 'cf_tracking_firefox5': 1,
 'cf_tracking_firefox56': 1,
 'cf_tracking_firefox57': 1,
 'cf_tracking_firefox6': 1,
 'cf_tracking_firefox7': 1,
 'cf_tracking_firefox71': 1,
 'cf_tracking_firefox_esr102': 1,
 'cf_tracking_firefox_esr115': 1,
 'cf_tracking_firefox_esr31': 1,
 'cf_tracking_firefox_relnote': 1,
 'cf_tracking_seamonkey229': 1,
 'cf_tracking_seamonkey253': 1,
 'cf_tracking_seamonkey257esr': 1,
 'cf_tracking_thunderbird_114': 1,
 'cf_tracking_thunderbird_115': 1,
 'cf_tracking_thunderbird_116': 1,
 'cf_tracking_thunderbird_38': 1,
 'cf_tracking_thunderbird_45': 1,
 'cf_tracking_thunderbird_91': 1,
 'cf_tracking_thunderbird_93': 1,
 'cf_tracking_thunderbird_esr102': 1,
 'cf_tracking_thunderbird_esr115': 1,
 'cf_tracking_thunderbird_esr38': 1,
 'cf_tracking_thunderbird_esr60': 1,
 'cf_tracking_thunderbird_esr78': 1,
 'cf_tracking_thunderbird_esr91': 1});


while (cursor.hasNext()) {
  var document = cursor.next();
  db.Tracking.insertOne(document);
}