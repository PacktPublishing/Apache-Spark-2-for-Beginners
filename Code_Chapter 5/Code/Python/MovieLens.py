
# coding: utf-8

# In[7]:

# Import all the required liraries 
from pyspark.sql import Row
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt
import pylab as P
get_ipython().magic('matplotlib inline')
plt.rcdefaults()


# In[8]:

# TODO - The following location has to be changed to the appropriate data file location
dataDir = "/Users/RajT/Documents/Writing/SparkForBeginners/To-PACKTPUB/Contents/B05289-05-SparkDataAnalysisWithPython/Data/ml-100k/"


# In[9]:

# Create the DataFrame of the user data set
lines = sc.textFile(dataDir + "u.user")
splitLines = lines.map(lambda l: l.split("|"))
usersRDD = splitLines.map(lambda p: Row(id=p[0], age=int(p[1]), gender=p[2], occupation=p[3], zipcode=p[4]))
usersDF = spark.createDataFrame(usersRDD)
usersDF.createOrReplaceTempView("users")
usersDF.show()


# In[10]:

# Create the DataFrame of the user data set with only one column age
ageDF = spark.sql("SELECT age FROM users")
ageList = ageDF.rdd.map(lambda p: p.age).collect()
ageDF.describe().show()


# In[11]:

# Age distribution of the users
plt.hist(ageList)
plt.title("Age distribution of the users\n")
plt.xlabel("Age")
plt.ylabel("Number of users")
plt.show(block=False)


# In[12]:

# Draw a probability density plot
from scipy.stats import gaussian_kde
density = gaussian_kde(ageList)
xAxisValues = np.linspace(0,100,1000) # Use the range of ages from 0 to 100 and the number of data points
density.covariance_factor = lambda : .5
density._compute_covariance()
plt.title("Age density plot of the users\n")
plt.xlabel("Age")
plt.ylabel("Density")
plt.plot(xAxisValues, density(xAxisValues))
plt.show(block=False)


# In[13]:

# The following example demonstrates the creation of multiple diagrams in one figure
# There are two plots on one row
# The first one is the histogram of the distribution 
# The second one is the boxplot containing the summary of the distribution
plt.subplot(121)
plt.hist(ageList)
plt.title("Age distribution of the users\n")
plt.xlabel("Age")
plt.ylabel("Number of users")
plt.subplot(122)
plt.title("Summary of distribution\n")
plt.xlabel("Age")
plt.boxplot(ageList, vert=False)
plt.show(block=False)


# In[14]:

occupationsTop10 = spark.sql("SELECT occupation, count(occupation) as usercount FROM users GROUP BY occupation ORDER BY usercount DESC LIMIT 10")
occupationsTop10.show()


# In[15]:

occupationsTop10Tuple = occupationsTop10.rdd.map(lambda p: (p.occupation,p.usercount)).collect()
occupationsTop10List, countTop10List = zip(*occupationsTop10Tuple)
occupationsTop10Tuple


# In[16]:

# Top 10 occupations in terms of the number of users having that occupation who have rated movies
y_pos = np.arange(len(occupationsTop10List))
plt.barh(y_pos, countTop10List, align='center', alpha=0.4)
plt.yticks(y_pos, occupationsTop10List)
plt.xlabel('Number of users')
plt.title('Top 10 user types\n')
plt.gcf().subplots_adjust(left=0.15)
plt.show(block=False)


# In[17]:

occupationsGender = spark.sql("SELECT occupation, gender FROM users")
occupationsGender.show()


# In[18]:

occCrossTab = occupationsGender.stat.crosstab("occupation", "gender")
occCrossTab.show()


# In[19]:

occCrossTab.describe('M', 'F').show()


# In[20]:

occCrossTab.stat.cov('M', 'F')


# In[21]:

occCrossTab.stat.corr('M', 'F')


# In[22]:

occupationsCrossTuple = occCrossTab.rdd.map(lambda p: (p.occupation_gender,p.M, p.F)).collect()
occList, mList, fList = zip(*occupationsCrossTuple)


# In[23]:

N = len(occList)
ind = np.arange(N)    # the x locations for the groups
width = 0.75          # the width of the bars: can also be len(x) sequence
p1 = plt.bar(ind, mList, width, color='r')
p2 = plt.bar(ind, fList, width, color='y', bottom=mList)
plt.ylabel('Count')
plt.title('Gender distribution by occupation\n')
plt.xticks(ind + width/2., occList, rotation=90)
plt.legend((p1[0], p2[0]), ('Male', 'Female'))
plt.gcf().subplots_adjust(bottom=0.25)
plt.show(block=False)


# In[24]:

occupationsBottom10 = spark.sql("SELECT occupation, count(occupation) as usercount FROM users GROUP BY occupation ORDER BY usercount LIMIT 10")
occupationsBottom10.show()


# In[25]:

occupationsBottom10Tuple = occupationsBottom10.rdd.map(lambda p: (p.occupation,p.usercount)).collect()
occupationsBottom10List, countBottom10List = zip(*occupationsBottom10Tuple)


# In[26]:

# Bottom 10 occupations in terms of the number of users having that occupation who have rated movies
explode = (0, 0, 0, 0,0.1,0,0,0,0,0.1)  
plt.pie(countBottom10List, explode=explode, labels=occupationsBottom10List, autopct='%1.1f%%', shadow=True, startangle=90)
plt.title('Bottom 10 user types\n')
plt.show(block=False)


# In[27]:

zipTop10 = spark.sql("SELECT zipcode, count(zipcode) as usercount FROM users GROUP BY zipcode ORDER BY usercount DESC LIMIT 10")
zipTop10.show()


# In[28]:

zipTop10Tuple = zipTop10.rdd.map(lambda p: (p.zipcode,p.usercount)).collect()
zipTop10List, countTop10List = zip(*zipTop10Tuple)


# In[29]:

# Top 10 zipcodes in terms of the number of users living in that zipcode who have rated movies
explode = (0.1, 0, 0, 0,0,0,0,0,0,0)  # explode a slice if required
plt.pie(countTop10List, explode=explode, labels=zipTop10List, autopct='%1.1f%%', shadow=True)
#draw a circle at the center of pie to make it look like a donut
centre_circle = plt.Circle((0,0),0.75,color='black', fc='white',linewidth=1.25)
plt.gcf().gca().add_artist(centre_circle)
# The aspect ratio is to be made equal. This is to make sure that pie chart is coming perfectly as a circle.
plt.axis('equal')
plt.text(- 0.25,0,'Top 10 zip codes')
plt.show(block=False)


# In[30]:

ages = spark.sql("SELECT occupation, age FROM users WHERE occupation ='administrator' ORDER BY age")
adminAges = ages.rdd.map(lambda p: p.age).collect()
ages.describe().show()


# In[31]:

ages = spark.sql("SELECT occupation, age FROM users WHERE occupation ='engineer' ORDER BY age")
engAges = ages.rdd.map(lambda p: p.age).collect()
ages.describe().show()


# In[32]:

ages = spark.sql("SELECT occupation, age FROM users WHERE occupation ='programmer' ORDER BY age")
progAges = ages.rdd.map(lambda p: p.age).collect()
ages.describe().show()


# In[33]:

# Box plots of the ages by profession
boxPlotAges = [adminAges, engAges, progAges]
boxPlotLabels = ['administrator','engineer', 'programmer' ]
x = np.arange(len(boxPlotLabels))
plt.figure()
plt.boxplot(boxPlotAges)
plt.title('Age summary statistics\n')
plt.ylabel("Age")
plt.xticks(x + 1, boxPlotLabels, rotation=0)
plt.show(block=False)


# In[34]:

movieLines = sc.textFile(dataDir + "u.item")
splitMovieLines = movieLines.map(lambda l: l.split("|"))
moviesRDD = splitMovieLines.map(lambda p: Row(id=p[0], title=p[1], releaseDate=p[2], videoReleaseDate=p[3], url=p[4], unknown=int(p[5]),action=int(p[6]),adventure=int(p[7]),animation=int(p[8]),childrens=int(p[9]),comedy=int(p[10]),crime=int(p[11]),documentary=int(p[12]),drama=int(p[13]),fantasy=int(p[14]),filmNoir=int(p[15]),horror=int(p[16]),musical=int(p[17]),mystery=int(p[18]),romance=int(p[19]),sciFi=int(p[20]),thriller=int(p[21]),war=int(p[22]),western=int(p[23])))
moviesDF = spark.createDataFrame(moviesRDD)
moviesDF.createOrReplaceTempView("movies")


# In[35]:

genreDF = spark.sql("SELECT sum(unknown) as unknown, sum(action) as action,sum(adventure) as adventure,sum(animation) as animation, sum(childrens) as childrens,sum(comedy) as comedy,sum(crime) as crime,sum(documentary) as documentary,sum(drama) as drama,sum(fantasy) as fantasy,sum(filmNoir) as filmNoir,sum(horror) as horror,sum(musical) as musical,sum(mystery) as mystery,sum(romance) as romance,sum(sciFi) as sciFi,sum(thriller) as thriller,sum(war) as war,sum(western) as western FROM movies")
genreList = genreDF.collect()
genreDict = genreList[0].asDict()
labelValues = list(genreDict.keys())
countList = list(genreDict.values())
genreDict


# In[36]:

# http://opentechschool.github.io/python-data-intro/core/charts.html
# Movie types and the counts
x = np.arange(len(labelValues))
plt.title('Movie types\n')
plt.ylabel("Count")
plt.bar(x, countList)
plt.xticks(x + 0.5, labelValues, rotation=90)
plt.gcf().subplots_adjust(bottom=0.20)
plt.show(block=False)


# In[37]:

yearDF = sqlContext.sql("SELECT substring(releaseDate,8,4) as releaseYear, count(*) as movieCount FROM movies GROUP BY substring(releaseDate,8,4) ORDER BY movieCount DESC LIMIT 10")
yearDF.show()
yearMovieCountTuple = yearDF.rdd.map(lambda p: (int(p.releaseYear),p.movieCount)).collect()
yearList,movieCountList = zip(*yearMovieCountTuple)
countArea = yearDF.rdd.map(lambda p: np.pi * (p.movieCount/15)**2).collect()


# In[38]:

# Top 10 years where the most number of movies have been released
plt.title('Top 10 movie release by year\n')
plt.xlabel("Year")
plt.ylabel("Number of movies released")
plt.ylim([0,max(movieCountList)])
colors = np.random.rand(10)
plt.scatter(yearList, movieCountList,c=colors)
plt.show(block=False)


# In[39]:

# Top 10 years where the most number of movies have been released
plt.title('Top 10 movie release by year\n')
plt.xlabel("Year")
plt.ylabel("Number of movies released")
plt.ylim([0,max(movieCountList) + 100])
colors = np.random.rand(10)
plt.scatter(yearList, movieCountList,c=colors, s=countArea)
plt.show(block=False)


# In[40]:

yearActionDF = spark.sql("SELECT substring(releaseDate,8,4) as actionReleaseYear, count(*) as actionMovieCount FROM movies WHERE action = 1 GROUP BY substring(releaseDate,8,4) ORDER BY actionReleaseYear DESC LIMIT 10")
yearActionDF.show()
yearActionDF.createOrReplaceTempView("action")


# In[41]:

yearDramaDF = spark.sql("SELECT substring(releaseDate,8,4) as dramaReleaseYear, count(*) as dramaMovieCount FROM movies WHERE drama = 1 GROUP BY substring(releaseDate,8,4) ORDER BY dramaReleaseYear DESC LIMIT 10")
yearDramaDF.show()
yearDramaDF.createOrReplaceTempView("drama")


# In[42]:

yearCombinedDF = spark.sql("SELECT a.actionReleaseYear as releaseYear, a.actionMovieCount, d.dramaMovieCount FROM action a, drama d WHERE a.actionReleaseYear = d.dramaReleaseYear ORDER BY a.actionReleaseYear DESC LIMIT 10")
yearCombinedDF.show()


# In[43]:

yearMovieCountTuple = yearCombinedDF.rdd.map(lambda p: (p.releaseYear,p.actionMovieCount, p.dramaMovieCount)).collect()
yearList,actionMovieCountList,dramaMovieCountList = zip(*yearMovieCountTuple)


# In[44]:

plt.title("Movie release by year\n")
plt.xlabel("Year")
plt.ylabel("Movie count")
line_action, = plt.plot(yearList, actionMovieCountList)
line_drama, = plt.plot(yearList, dramaMovieCountList)
plt.legend([line_action, line_drama], ['Action Movies', 'Drama Movies'],loc='upper left')
plt.gca().get_xaxis().get_major_formatter().set_useOffset(False)
plt.show(block=False)


# In[ ]:



