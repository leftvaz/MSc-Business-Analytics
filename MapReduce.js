//1

var map = function() {  
    var courses = this.courses;
    for (var idx = 0; idx < this.courses.length; idx++) {
                           var course_title = this.courses[idx].course_title;
    if (course_title) { 
        course_title = course_title.toLowerCase().split(" "); 
        for (var i = course_title.length - 1; i >= 0; i--) {
            if(course_title[i].match("for|to|in|of|and")) {  //could add more but these are the only ones in the docs
                void(0)
            }
            
            else if (course_title[i])  {      
               emit(course_title[i], 1); 
            }
        }
    }}
};

var reduce = function( key, values ) {    
    var count = 0;    
    values.forEach(function(v) {            
        count +=v;    
    });
    return count;
};

db.students.mapReduce(map, reduce, {out: "word_count"});

db.word_count.find().sort({value:-1})


//2

var map = function() {  
    var courses = this.courses;
    var course;
    var key;
    if (courses) { 
        for (var idx = courses.length - 1; idx >= 0; idx--) {
            course= courses[idx]
            if (course.course_status==="Complete") {
                key = {
                    homeCity:this.home_city,
                    courseType:course.course_title.charAt(0)
                };
            emit(key, course.grade);   
            }
        }
    }
};

var reduce = function( key, values ) {      
    return (Array.sum(values)/values.length);
}
                    
db.students.mapReduce( map,reduce,{
    out: { merge: "average_scores" },
});

db.average_scores.aggregate([
    {$project : {_id: "$_id", average_score: "$value"}},
    {$sort: {average_score:-1}}
    ])




