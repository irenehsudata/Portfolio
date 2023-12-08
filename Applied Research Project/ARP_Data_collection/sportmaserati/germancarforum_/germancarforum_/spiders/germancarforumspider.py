import scrapy
from urllib.parse import urlencode

#from scrapy.http import FormRequest
API_KEY = "87448ff5-54bf-43bd-b529-9161aa3cf5d6"

def get_proxy_url(url):

    payload = {"api_key": API_KEY, "url": url}
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)

    return proxy_url

class GermancarforumspiderSpider(scrapy.Spider):
    name = "germancarforumspider"
    #allowed_domains = ["www.germancarforum.com"]
    #start_urls = ["https://www.germancarforum.com/"]

    def start_requests(self):
        start_url = "https://www.sportsmaserati.com/index.php"
        yield scrapy.Request(url = get_proxy_url(start_url), callback = self.parse)

# IN MAIN PAGE:

    # get sub-section link
    def parse(self, response):
        sub_section_link = response.css("div.node-body")
        
        for sl in sub_section_link:
            
            relative_sub_section_url = sl.css("h3 a ::attr(href)").get()

            # exclude certain sub-section link
            excluded_urls = ["/index.php?forums/announcements.7/", "/index.php?forums/australia.27/", "/index.php?forums/events.81/", "/index.php?forums/humour-jokes-and-other-topics.22/","/index.php?forums/usa.82/"]
            if relative_sub_section_url not in excluded_urls:
                sl_url = "https://www.sportsmaserati.com/" + relative_sub_section_url

                yield response.follow(get_proxy_url(sl_url), 
                                    callback = self.parse_sub_section_page)

# IN SUB-SECTION PAGE:

    # get thread link
    def parse_sub_section_page(self, response):

        thread_link = response.css("li.structItem-startDate")

        for tl in thread_link:

            relative_thread_url = tl.css("a ::attr(href)").get()    
            tl_url = "https://www.sportsmaserati.com/" + relative_thread_url

            yield response.follow(get_proxy_url(tl_url),
                                  callback = self.parse_thread_page)
        
        # get next page link
        next_page = response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]/@href').get()
        if next_page is not None:
            next_page_url = "https://www.sportsmaserati.com/" + next_page

            yield response.follow(get_proxy_url(next_page_url), 
                                        callback = self.parse_sub_section_page)

# IN THREAD PAGE:

    def parse_thread_page(self, response):

        # section_name = response.css('ul.p-breadcrumbs li[itemprop="itemListElement"] a span::text').getall()[1]
        section_name = response.xpath('//li[@itemprop="itemListElement"]/a[@href="/index.php?forums/forum-chat.9/"]/span[@itemprop="name"]/text()').get()
        thread_title = response.css('h1.p-title-value::text').get().strip()
        thread = response.xpath("//article[contains(@class,'message') and contains(@class,'message--post') and contains(@class,'js-post') and contains(@class,'js-inlineModContainer')]")

        for post in thread:
            post_ID = post.css("article.message::attr(data-content)").get()
            post_date = post.css("a time ::text").get()
            post_username = post.css("h4 a ::text").get()
            post_text_not_clean = [list(filter(None, [i.strip() for i in post.css("article.message-body.js-selectToQuote div.bbWrapper::text").getall()]))]
            post_text = []
            post_text.extend(post_text_not_clean[0])
            post_order = post.css("ul.message-attribution-opposite li:nth-child(2) a::text").get().strip()
            replier_post_ID = post.xpath('.//div[@class="bbWrapper"]/blockquote/@data-source').getall()
            
            # get user profile link
            relative_user_profile_url = post.css("h4.message-name a::attr(href)").get()
            if relative_user_profile_url is not None:
                up_url = "https://www.sportsmaserati.com/" + relative_user_profile_url

                yield response.follow(get_proxy_url(up_url), 
                                      callback = self.parse_user_profile_page, 
                                      cb_kwargs={
                                        "section_name": section_name,
                                        "thread_title": thread_title,
                                        "post_ID": post_ID,
                                        "post_date": post_date,
                                        "post_username": post_username,
                                        "post_text": post_text,
                                        "post_order": post_order,
                                        "replier_post_ID": replier_post_ID})
            
        # get next post page link
        next_post_page = response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]/@href').get()
        if next_post_page is not None:
            next_post_page_url = "https://www.sportsmaserati.com/" + next_post_page

            yield response.follow(get_proxy_url(next_post_page_url), 
                                  callback = self.parse_thread_page)

# IN USER PROFILE PAGE:

    def parse_user_profile_page(self, response, section_name, thread_title, post_ID, post_date, post_username, post_text, post_order, replier_post_ID):
        login_filter = response.css('body::attr(data-template)').get()
        if login_filter == "member_view":

            # personal info
            user_birthday = response.xpath('//dl[@class="pairs pairs--columns pairs--fixedSmall"]/dd/text()').get(default="NA").strip()
            user_location = response.css("div.memberHeader-blurb a::text").get(default="NA")
            user_gender = response.css('dl[data-field="gender"] dd::text').get(default="NA").strip()
            user_occupation = response.css('dl[data-field="occupation"] dd::text').get(default="NA").strip()

            # engagement
            user_messages_count = response.xpath('//dl[contains(dt/text(), "Messages")]/dd/a/text()').get(default="NA").strip()
            user_reaction_score = response.xpath('//dl[contains(dt/text(), "Reaction score")]/dd/text()').get(default="NA").strip()

            # bio + signature
            user_about_original = response.css('div.block-body div.block-row.block-row--separated div.bbWrapper::text').getall()
            if user_about_original is not None:
                user_about_not_clean = [list(filter(None, [i.strip() for i in user_about_original]))]
                user_about = []
                user_about.extend(user_about_not_clean[0])

            # member type
            user_member_type = response.css("em strong::text").get(default="NA")

            # cars
            user_automobile_original = response.css('dl[data-field="automobile"] dd::text').getall()
            user_mygarage_original = response.css('dl[data-field="my_garage"] dd::text').getall()
            user_oldcars_original = response.css('dl[data-field="gone_but_not_forgotten"] dd::text').getall()
            user_dreamcars_original = response.css('dl[data-field="my_dream_car"] dd::text').getall()

            user_car = []
            if user_automobile_original is not None:
                user_automobile_not_clean = [list(filter(None, [garage.strip() for garage in user_automobile_original]))]
                user_automobile = []
                user_automobile.extend(user_automobile_not_clean[0])
                user_car.extend(user_automobile)

            if user_mygarage_original is not None:
                user_mygarage_not_clean = [list(filter(None, [garage.strip() for garage in user_mygarage_original]))]
                user_mygarage = []
                user_mygarage.extend(user_mygarage_not_clean[0])
                user_car.extend(user_mygarage)
    
            if user_oldcars_original is not None:
                user_oldcars_not_clean = [list(filter(None, [oldcars.strip() for oldcars in user_oldcars_original]))]
                user_oldcars = []
                user_oldcars.extend(user_oldcars_not_clean[0])
                user_car.extend(user_oldcars)
    
            if user_dreamcars_original is not None:
                user_dreamcars_not_clean = [list(filter(None, [dreamcars.strip() for dreamcars in user_dreamcars_original]))]
                user_dreamcars = []
                user_dreamcars.extend(user_dreamcars_not_clean[0])
                user_car.extend(user_dreamcars)


        yield {
                "section_name": section_name,
                "thread_title": thread_title,
                "post_ID": post_ID,
                "post_date": post_date,
                "post_username": post_username,
                "post_text": post_text,
                "post_order": post_order,
                "replier_post_ID": replier_post_ID,
                "user_birthday": user_birthday,
                "user_location": user_location,
                "user_gender": user_gender,
                "user_occupation": user_occupation,
                "user_messages_count": user_messages_count,
                "user_reaction_score": user_reaction_score,
                "user_car": user_car,
                "user_about": user_about,
                "user_member_type": user_member_type
                }