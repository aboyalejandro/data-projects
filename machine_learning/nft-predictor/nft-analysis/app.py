from flask import Flask, render_template, request
from data import nfts
from recos import new_collections_dict,nfts_recos,new_collections
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
import scipy.stats as st
import pandas as pd 

app = Flask(__name__)

@app.route('/')

def index():
    return """<html lang="en">
                <head>
                    <meta charset="utf-8">
                    <title>NFT Analysis</title>
                    <link rel="stylesheet" href="./static/css/main.css") }}">
                </head>
                <body>
              <h1> Hello, check your NFTs before investing &#128269 </h1>
              <ul>
              <li><a href="/overview"> Click here to have a quick overview of a category and chain. </a></li>
              <li><a href="/comparisons-categories"> Click here to compare 2 different categories based on their features. </a></li>
              <li><a href="/comparisons-chains"> Click here to compare 2 different chains based on their features. </a></li>
              <li><a href="/recommendations"> Click here to try some random tests and predict your new nfts. </a></li>
              </ul>  
                </body>
           """

@app.route('/overview',methods=['GET', 'POST'])

def nft_overview():
    if request.method == "GET":
        return render_template('overview.html',my_result= "Please submit an option.")

    if request.method == "POST":
        chain = request.form['blockchain']
        category = request.form['category']

        chain_selection = chain
        category_selection = category

        nfts_selected = [i for i in nfts if i['Category'] == category and i['Chain'] == chain]

        return render_template('overview.html',
                               chain= chain_selection,
                               category=category_selection,
                               nfts = nfts_selected)

@app.route('/recommendations',methods=['GET', 'POST'])

def nft_recommendations():
    if request.method == "GET":
        result = "Please submit an option."
        return render_template('recommendations.html',my_result= result)

    if request.method == "POST":
        feature = request.form['feature']
        feature_selection = feature

        X = nfts_recos[['7 d %','Twitter followers','Ownership %','Floor (USD)','Volume (USD) in M','Market Share %']]
        Y = pd.DataFrame(data=nfts_recos, columns=[feature])
        transformer = StandardScaler().fit(X)
        scaled_x = pd.DataFrame(transformer.transform(X),columns = X.columns)
        model = linear_model.LogisticRegression(random_state=0)
        result = model.fit(scaled_x, nfts_recos[feature])
        accuracy_output = int((accuracy_score(result.predict(scaled_x),Y)*100))
        results = list(result.predict(new_collections))
        results_final = ', '.join(results)
    
        def comments(accuracy_output): 

            if accuracy_output > 75:
                return f"Your tests are likely to be right!"
            elif accuracy_output > 65:
                return f"Your are more likely to be right, but not very trustable yet :("
            elif accuracy_output > 50:
                return f"We don't have enough values to make a good prediction, need more sampling :( "
            else:
                return f"We don't have enough values to make a good prediction. Try another feature! "

        return render_template('recommendations.html',
                               feature=feature_selection,
                               nfts = new_collections_dict,
                               accuracy=accuracy_output,
                               comments = comments(accuracy_output),
                               results = results
                               )

@app.route('/comparisons-categories',methods=['GET', 'POST'])

def nft_categories_comparison():

    def t_test_features(s1, s2, features=['24 hs %', '7 d %','Owners','Items','Twitter followers','Ownership %','Volume (USD) in M','Floor (USD)','Gas Volume (USD) in M','Market Share %']):
        results = {k:st.ttest_ind(s1[k], s2[k])[1] for k in features}   
        return results

    if request.method == "GET":
        return render_template('comparisons-categories.html',my_result= "Please submit an option.")

    if request.method == "POST":
  
            category1 = request.form['category1']
            category2 = request.form['category2']
            cat1_filter = nfts_recos[nfts_recos['Category'] == category1]
            cat2_filter = nfts_recos[nfts_recos['Category'] == category2]

    comparison = t_test_features(cat1_filter,cat2_filter)

    avg_comparison = 0
    for val in comparison.values():
        avg_comparison += val
    
    # using len() to get total keys for mean computation
    avg_comparison = round((avg_comparison / len(comparison)),2)*100
    max_rate_value = round(max(comparison.values()),2)*100
    min_rate_value = round(min(comparison.values()),2)*100
    max_rate_key =  max(comparison, key=comparison.get)
    min_rate_key = min(comparison, key=comparison.get)

    def conclusions(avg_comparison,max_rate_key,):

        if avg_comparison < 11:
            return f"They are not that different, you might want to try another comparison."
        elif avg_comparison > 11:
            return f"This is starting to look interesting, watch out for {max_rate_key}! "
        elif avg_comparison > 25:
            return f"There are some interesting differences, pay special attention to {max_rate_key} when investing! "
        elif avg_comparison > 35:
            return f"Considering them as 2 very different scenarios when expecting results. {max_rate_key} is pulling up all the other features." 


    return render_template('comparisons-categories.html',
                               category1=category1,
                               category2=category2,
                               comparison=comparison,
                               rate=avg_comparison,
                               max_rate = max_rate_value,
                               min_rate = min_rate_value,
                               max_key = max_rate_key,
                               min_key = min_rate_key,
                               conclusions=conclusions(avg_comparison,max_rate_key)
                               )

@app.route('/comparisons-chains',methods=['GET', 'POST'])
                              
def nft_chains_comparison():

    def t_test_features(s1, s2, features=['24 hs %', '7 d %','Owners','Items','Twitter followers','Ownership %','Volume (USD) in M','Floor (USD)','Gas Volume (USD) in M','Market Share %']):
        results = {k:st.ttest_ind(s1[k], s2[k])[1] for k in features}   
        return results

    if request.method == "GET":
        return render_template('comparisons-chains.html',my_result= "Please submit an option.")

    if request.method == "POST":
            chain1 = request.form['chain1']
            print(chain1)
            chain2 = request.form['chain2']
            print(chain2)
            ch1_filter = nfts_recos[nfts_recos['Chain'] == chain1]
            ch2_filter = nfts_recos[nfts_recos['Chain'] == chain2]

    comparison = t_test_features(ch1_filter,ch2_filter)

    avg_comparison = 0
    for val in comparison.values():
        avg_comparison += val
    
    # using len() to get total keys for mean computation
    avg_comparison = round((avg_comparison / len(comparison)),2)*100
    max_rate_value = round(max(comparison.values()),2)*100
    min_rate_value = round(min(comparison.values()),2)*100
    max_rate_key =  max(comparison, key=comparison.get)
    min_rate_key = min(comparison, key=comparison.get)

    def conclusions(avg_comparison,max_rate_key):

        if avg_comparison < 11:
            return f"They are not that different, you might want to try another comparison."
        elif avg_comparison > 11:
            return f"This is starting to look interesting, watch out for {max_rate_key}! "
        elif avg_comparison > 25:
            return f"There are some interesting differences, pay special attention to {max_rate_key} when investing! "
        elif avg_comparison > 35:
            return f"Considering them as 2 very different scenarios when expecting results. {max_rate_key} is pulling up all the other features." 

    return render_template('comparisons-chains.html',
                               chain1=chain1,
                               chain2=chain2,
                               comparison=comparison,
                               rate=avg_comparison,
                               max_rate = max_rate_value,
                               min_rate = min_rate_value,
                               max_key = max_rate_key,
                               min_key = min_rate_key,
                               conclusions=conclusions(avg_comparison,max_rate_key)
                               )                                                            

app.run(debug=True)


