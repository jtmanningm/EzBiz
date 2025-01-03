import requests

def send_simple_message():
  	return requests.post(
  		"https://api.mailgun.net/v3/sandbox16a2d63b058f4bbc914143c47438384a.mailgun.org/messages",
  		auth=("api", "YOUR_API_KEY"),
  		data={"from": "Excited User <mailgun@sandbox16a2d63b058f4bbc914143c47438384a.mailgun.org>",
  			"to": ["bar@example.com", "YOU@sandbox16a2d63b058f4bbc914143c47438384a.mailgun.org"],
  			"subject": "Hello",
  			"text": "Testing some Mailgun awesomeness!"})