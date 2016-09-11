##Question 1
install.packages("rmongodb")
library(rmongodb)

##Question 2
mongo<-mongo.create()

##Question 3
mongo.is.connected(mongo)

##Question 4
namespace <- "r.lab2"

##Question 5
christiano<-'{"name":"Christiano", "language":"Portugese"}'

##Question 6
ok<-mongo.insert(mongo, namespace, christiano)
ok

##Question 7
ioanna<- list(name="Ioanna", language="English", age="34")
dimitris<- list(name="Dimitris", language="Greek", age="29")
ioanna<- mongo.bson.from.list(ioanna)
dimitris<- mongo.bson.from.list(dimitris)

##Question 8
mongo.insert.batch(mongo, namespace, list(ioanna, dimitris))

##Question 9
mongo.update(mongo, namespace, list(name="Christiano"), list(name="Christiano", language="Portugese", age = 26) )

##Question 10
mongo.remove(mongo, namespace, list(name="Dimitris"))

##Question 11
lefteris<- list(name="Lefteris", language="Greek", age="23")
miguel<- list(name="Miguel", language="English", age="47")
lefteris<- mongo.bson.from.list(lefteris)
miguel<- mongo.bson.from.list(miguel)
mongo.insert.batch(mongo, namespace, list(lefteris, miguel))


cursor <- mongo.find(mongo, namespace)
current_row_number <- 0
names<-NULL
languages<-NULL
ages<-NULL
while(mongo.cursor.next(cursor)) {
  current_row_number<- current_row_number+1
  current_row<-mongo.cursor.value(cursor)
  names <- append(names, mongo.bson.value(current_row,"name"))
  languages <- append(languages, mongo.bson.value(current_row,"language"))
  ages <- append(ages, mongo.bson.value(current_row,"age"))
}
df = data.frame(names, languages, ages)
df

##Question 12
ns<-"r.heart"
heart<-read.csv("https://archive.ics.uci.edu/ml/machine-learning-databases/statlog/heart/heart.dat", header = F, sep = " ")
head(heart)
heart<-read.csv("https://archive.ics.uci.edu/ml/machine-learning-databases/statlog/heart/heart.dat", header = F, sep = " ")
names(heart) [1:14] <- c("age", "sex", "chest_pain_type", "blood_pressure", "cholestoral","blood_sugar", "ecg_results", "max_heart_rate", "angina", "oldpeak", "slope", "vessels", "thal", "idk" ) 
heart<-heart[1:13]
head(heart)

for (i in 1:nrow(heart)){
  patient<-list(age=heart[i,]$age,sex=heart[i,]$sex, chest_pain_type=heart[i,]$chest_pain_type, blood_pressure=heart[i,]$blood_pressure, cholestoral=heart[i,]$cholestoral, blood_sugar=heart[i,]$blood_sugar, ecg_results=heart[i,]$ecg_results, max_heart_rate=heart[i,]$max_heart_rate, angina=heart[i,]$angina, oldpeak=heart[i,]$oldpeak, slope=heart[i,]$slope, vessels=heart[i,]$vessels, thal=heart[i,]$thal  )
  p<-mongo.bson.from.list(patient)
  mongo.insert.batch(mongo, ns, list(p))
}

##Question 13
mongo.destroy(mongo)
mongo.is.connected(mongo)


