#import the necessary packages and make sure its installed before use it
library(purrr)		
library(highcharter)
library(shiny)
library(shinydashboard)
library(dplyr)
library(rCharts)
library(reshape2)
library(sparklyr)

#create spark context
sc <- spark_connect(master   = "yarn-client",
                    app_name = "sparklyr",
                    version  = "2.3.1",
                    spark_home = "/usr/local/spark/spark-2.3.1-bin-hadoop2.7")

#load data from hdfs into R(read about R)
rmkdf <- spark_read_csv(sc, name = "rmktab", path = "hdfs://localhost:8020/spark/*.csv", header = FALSE,infer_schema = TRUE)
#set column names to R dataframe
names(rmkdf)<-c("Customer_Id","Gender","District","State","DOB","Marital_Status","Anniversary","Category")

#filter case 1 :tot no of customer
total.customer <- count(rmkdf)
totcus<- collect(total.customer)
#filter case 2 :tot no of married customer
married.customer<- rmkdf %>% filter(Marital_Status=="M") %>% count()
marcus<- collect(married.customer)
#filter case 3 :tot no of unmarried customer
unmarried.customer <- rmkdf %>% filter(Marital_Status=="S") %>% count()
unmarcus<- collect(unmarried.customer)


#create shinny Dash board UI: in shinny it has two phase UI to view and Server to process. Here did first step with sake simple
#UI will have customer analysis of 3 level drill down in bar chart, age analysis in line chart and buttons to show filter case results
#UI has three part 1. dashboardHeader, 2. dashboardSidebar and 3. dashboardBody
ui <- dashboardPage(skin = "red",
  dashboardHeader(title = "RMKV Dashboard",tags$li(a(href = 'https://www.x.com',
                                                  img(src = 'logo.png',		#file should be in 'www' named folder in the location where sparklyrPOC will be saved
                                                         title = "Company Home",height = 30,width=70),
                                                     style = "padding-top:10px; padding-bottom:10px;"),
                                                   class = "dropdown")),
  dashboardSidebar(sidebarMenu(menuItemOutput("menuitem"),selectInput("choo", "State", c(choices = "","TAMIL NADU", "PONDICHERRY", "KARNATAKA"), selectize = TRUE),sliderInput("range","Age Range",min = 10,max = 90,value = c(20,80)))),
  dashboardBody( 
    fluidRow(
      infoBox("Total Customers", totcus$n, icon = icon("stats",lib='glyphicon'), color="green",fill = TRUE),
      infoBox("Married Customers", marcus$n, icon = icon("pie-chart"), color="purple",fill = TRUE),
      infoBox("Unmarried Customers",unmarcus$n, icon = icon("bar-chart-o"), color="orange",fill = TRUE)
    ),
    fluidRow(box(title = "Marrital Analysis", status = "primary",solidHeader = TRUE,collapsible = TRUE,highchartOutput("chart", "auto")),
                    tabBox(title ="Age Analysis ",tabPanel("Graph",showOutput("mychart","highcharts")),
                                                  tabPanel("Table",dataTableOutput("table2")))))
                    #tabbox(title = "Age Analysis", status = "primary",solidHeader = TRUE,collapsible = TRUE,plotOutput("plot1", height = "1px"),showOutput("mychart","highcharts"))))
)


#the server phase has original code to analyse data
server <- function(input, output, session) {
  output$menuitem <- renderMenu({
    menuItem("Menu item", icon = icon("list"),href = 'https://www.xyz.com/')
  })
#update user inputs
  observe({
    updateSelectInput(session, "choo")
    updateSliderInput(session, "range")
    ab=as.character(input$choo)
  })
#analyse customer data for 3 level of drill down bar chart based user input
  output$chart <- renderHighchart({
    ab=as.character(input$choo)
    gender_wise <- rmkdf %>%group_by(Gender) %>%filter(Gender=="M"||Gender=="F") %>%summarise(tot=count())%>% collect
    df <- data_frame(name =c("Female","Male"),y = gender_wise$tot,drilldown = tolower(name))
    ds <- list_parse(df)
    hc <- highchart() %>%hc_exporting(enabled = TRUE,show=FALSE) %>% hc_chart(type = "column",options3d = list(enabled = TRUE, beta = 45, alpha = 15)) %>% hc_title(text = "Customer Details") %>%hc_add_theme(hc_theme_ffx()) %>%hc_xAxis(type = "category") %>%hc_legend(enabled = FALSE) %>%hc_plotOptions(series = list(boderWidth = 0,dataLabels = list(enabled = TRUE))) %>%hc_add_series(name = "Things",colorByPoint = TRUE,data = ds)
    m_Status = rmkdf %>%group_by(Marital_Status) %>%filter(Gender == "F")%>%filter(Marital_Status =="M"||Marital_Status=="S")%>%summarise(tot1 = count())%>%collect
    m_Status %>% tbl_df
    sm <- data_frame(name = c("M-Married","M-Unmarried"),y= m_Status$tot1,drilldown = tolower(name))
    f_Status = rmkdf %>%group_by(Marital_Status) %>%filter(Gender == "M")%>%filter(Marital_Status =="M"||Marital_Status=="S")%>%summarise(tot1 = count())%>% collect
    f_Status %>% tbl_df
    sf <- data_frame(name = c("F-Married","F-Unmarried"),y = f_Status$tot1,drilldown = tolower(name))
   
    
    print(input$choo)
    # stmm <- rmkdf %>% filter(State== input$choo && Gender=="M"&& Marital_Status=="M") %>% group_by(District) %>%summarise(tot=count())%>% top_n(4)%>% collect
    #hcmm <- data_frame( name = stmm$District,value = stmm$tot)
    
    #stms <- rmkdf %>% filter(State== input$choo && Gender=="M"&&Marital_Status=="S") %>% group_by(District) %>%summarise(tot=count())%>% top_n(4)%>% collect
    #hcms <- data_frame( name = stms$District,value = stms$tot)
     #stfm <- rmkdf %>% filter(State== input$choo && Gender=="F"&&Marital_Status=="M") %>% group_by(District) %>%summarise(tot=count())%>%top_n(4)%>%collect
    # hcfm <- data_frame( name = stfm$District,value = stfm$tot)
    #stfs <- rmkdf %>% filter(State== input$choo && Gender=="F"&&Marital_Status=="S") %>%group_by(District) %>% summarise(tot=count())%>% top_n(4)%>% collect
    #hcfs <- data_frame( name = stfs$District,value = stfs$tot)
    second_el_to_numeric <- function(ls){
      map(ls, function(x){
        x[[2]] <- as.numeric(x[[2]])
        x
      })
    }
    
   #d_sm1 <- list_parse(sm)
    #d_sf1 <- list_parse(sf)
    #hc_mm1 <- list_parse2(hcmm)
    #hc_ms1 <- list_parse2(hcms)
    #hc_fm1 <- list_parse2(hcfm)
    #hc_fs1 <- list_parse2(hcfs)
    
 
   
    hc <- hc %>% hc_drilldown(allowPointDrilldown = TRUE,series = list(list(id = "male",name = "Male",data = list_parse(sm),color="#8117b2"),
                                                                      list(id = "female",name = "Female",data = list_parse(sf),color="#d65b22"),
                                                                      list(id = "m-married",data = second_el_to_numeric(list_parse2(rmkdf %>% filter(State == input$choo && Gender=="M"&& Marital_Status=="M") %>% group_by(District) %>%summarise(tot=count())%>% top_n(4)%>% collect)),color="#7c0901"),
                                                                      list(id = "m-unmarried",data = second_el_to_numeric(list_parse2(rmkdf %>% filter(State == input$choo && Gender=="M"&&Marital_Status=="S") %>% group_by(District) %>%summarise(tot=count())%>% top_n(4)%>% collect)),color="#241872"),
                                                                      list(id = "f-married",data = second_el_to_numeric(list_parse2(rmkdf %>% filter(State == input$choo && Gender=="F"&&Marital_Status=="M") %>% group_by(District) %>%summarise(tot=count())%>%top_n(4)%>%collect)),color="#ce4ce8"),
                                                                      list(id = "f-unmarried",data = second_el_to_numeric(list_parse2(rmkdf %>% filter(State == input$choo && Gender=="F"&&Marital_Status=="S") %>%group_by(District) %>% summarise(tot=count())%>% top_n(4)%>% collect)),color="#4f8cef")))
       })
  
 #use case two tot no of customer for range in age, based on user input 
  output$mychart <- renderChart2({
  agedf1<-rmkdf %>% mutate(Age = 2018- as.numeric(substr(DOB,1,4))) %>% filter((Age>=input$range[1] & Age<=input$range[2]) && (Gender =="M"||Gender =="F")) %>% select(Gender,Age,Customer_Id) %>%group_by(Gender,Age) %>% count() %>% collect
  h1 <- hPlot(n ~ Age , group = "Gender",data = agedf1, type = "line", group = "Gender")
  h1$set(width = session$clientData$output_plot1_width)
  return(h1)
  })
#to show the data table  
  output$table2 <- renderDataTable(agedf1)
  
 }
#to call phases
shinyApp(ui, server)
