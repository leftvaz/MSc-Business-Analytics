
//Question 1
db.createCollection("students",load("prep.js"))
db.students.findOne()

//Question 2
//1
db.students.find({courses:{$elemMatch:{course_status: 'In Progress'}}}).count()

//2
db.students.aggregate([
	{$group : {_id: "$home_city", num_students: {$sum : 1}}}
	])

//3            
db.students.aggregate([                                       
	{ $unwind: "$hobbies" },
	{ $group : {"_id":"$hobbies", counts:{$sum:1} }}, 
	{$group : {"_id": "$counts", "best_hobbies": {$push:"$_id"}}},
	{$sort: {counts:-1}}, 
	{ $limit : 1 },
	{$project: {"_id" : 0,"best_hobbies": 1}}
 ])


//4
db.students.aggregate([
    { $unwind: "$courses" },
    { $match: { "courses.course_status": "Complete"} },
    { $group: { "_id": "$_id", "GPA": { $avg: "$courses.grade" } } },
    {$sort: {GPA:-1}}, 
	{ $limit : 1 },
	{$project: {_id:0 , "GPA":1}}
])                    

//5
db.students.aggregate([
    { $unwind: "$courses" },
    { $match: { "courses.grade": 10} },
    { $group : {_id:"$_id", count:{$sum:1} }},
    {$group : {"_id": "$count", "best_students": {$push:"$_id"}}},
    {$sort: {count:-1}}, 
	{ $limit : 1 },
	{$project: {"_id":1, "best_students": 1}}
]).pretty()

//6
db.students.aggregate([
    { $unwind: "$courses" },
    { $match: { "courses.course_status": "Complete"} },
    { $group: { "_id": "$courses.course_title", "GPA": { $avg: "$courses.grade" } } },
    {$group : {"_id": "$GPA", "best_classes": {$push:"$_id"}}},
    {$sort: {GPA:-1}}, 
	{ $limit : 1 },
	{$project: {"_id":0, "best_classes": 1}}
]) 

//7
db.students.aggregate([
    { $unwind: "$courses" },
    { $match: { "courses.course_status": "Dropped"} },
    { $group: { "_id": "$courses.course_title", count:{$sum:1} } } ,
    {$group : {"_id": "$count", "most_dropped_classes": {$push:"$_id"}}},
    {$sort: {count:-1}}, 
	{ $limit : 1 },
	{$project: {"_id":0, "most_dropped_classes": 1}}
]) 

//8
db.students.aggregate([
    { $unwind: "$courses" },
    { $match: { "courses.course_status": "Complete"} },
    { $group: { "_id": { $substr: [ "$courses.course_code", 0, 1 ] }, count:{$sum:1} } } ,
    {$sort: {count:-1}}, 
	{ $limit : 1 },
	{$project: {"_id":1}}
])


//9             
db.students.aggregate([
	{ $unwind: "$hobbies" },
	{ $group : {_id:"$_id",count:{$sum:1}, home_city: {$first: "$home_city"}, first_name: {$first: "$first_name"}, hobbies: {$push:"$hobbies" }, favourite_os:{$first: "$favourite_os"}, laptop_cost:{$first: "$laptop_cost"}, courses:{$first: "$courses"}}},
	{$project:{_id: 1, home_city:1, first_name:1, hobbies: 1,favourite_os:1, laptop_cost:1, courses: 1, hobbyist:
               {
                 $cond: { if: { $gt: [ "$count", 3 ] }, then: true, else: false }
               } 
           }}
]).pretty()     //if we want the transformation to go through (i.e. be saved) we can use .out()


//10        
db.students.aggregate([
    { $unwind: "$courses" },
    { $match: { "courses.course_status": "Complete"} },
    { $group: { "_id": "$_id", classes_completed: {$sum:1}, home_city: {$first: "$home_city"}, first_name: {$first: "$first_name"}, hobbies: {$first:"$hobbies" }, favourite_os:{$first: "$favourite_os"}, laptop_cost:{$first: "$laptop_cost"}, courses:{$push: "$courses"} } } ,
    {$project:{_id: 1, home_city:1, first_name:1, hobbies: 1,favourite_os:1, laptop_cost:1, courses: 1, classes_completed: 1}}
]).pretty()


//11				
db.students.aggregate([
	{$project: 
		{first_name:1,
		 GPA:{$avg:"$courses.grade"}, 
		 classesInProgress:{$size: 
		 	{$filter: {input: "$courses", as: "courses", cond: { $eq: [ "$$courses.course_status", "In Progress" ] }}}},
		 droppedClasses:{$size: 
		 	{$filter: {input: "$courses", as: "courses", cond: { $eq: [ "$$courses.course_status", "Dropped" ] }}}}
		}
	}
	]).pretty()


//12
db.students.aggregate([
		{ $unwind: "$courses" },
		{ $group: { "_id": "$courses.course_code", "course_title": {$first:"$courses.course_title"}, "course_status":{$push:"$courses.course_status"}, "num_students":{$push:"$_id"}, "grades":{$push:"$courses.grade"} }}, 
		{$project: {"_id": 1, course_title: 1,
					numberOfDropouts:{$size: {$filter: {input: "$course_status", as: "c1", cond: { $eq: [ "$$c1", "Dropped" ] }}}},
					numberOfTimesCompleted:{$size: {$filter: {input: "$course_status", as: "c2", cond: { $eq: [ "$$c2", "Complete" ] }}}},
					currentlyRegistered: "$num_students",
					maxGrade: {$max: "$grades"},
					minGrade: {$min: "$grades"},
					avgGrade: {$avg: "$grades"}
		}},
		{$out: "huge_grasshopper"}    //going to hell for this
])