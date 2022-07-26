from datetime import date, timedelta, datetime

month_values = {
    "January"       : "01",
    "February"      : "02",
    "March"         : "03",
    "April"         : "04",
    "May"           : "05",
    "June"          : "06",
    "July"          : "07",
    "August"        : "08",
    "September"     : "09",
    "October"       : "10",
    "November"      : "11",
    "December"      : "12",
}

def get_month_name(month_number_string):
    try:
        for key, value in month_values.items():
            if month_number_string == value:
                return key
    except Exception as e:
        return {"Status": "Error", "Message": str(e)}

def get_month_value(month_name_string):
    try:
        month_name = month_values[month_name_string]
        return month_name
    except Exception as e:
        return {"Status": "Error", "Message": str(e)}

def days_cur_month():
    try:
        m = datetime.now().month
        y = datetime.now().year
        if m == 12:
            ndays = (date(y+1, 1, 1) - date(y, m, 1)).days
            d1 = date(y, m, 1)
            d2 = date(y, m, ndays)
            delta = d2 - d1

            return [str((d1 + timedelta(days=i)).strftime('%Y-%m-%d')) + " 00:00:00" for i in range(delta.days + 1)]
        else:
            ndays = (date(y, m+1, 1) - date(y, m, 1)).days
            d1 = date(y, m, 1)
            d2 = date(y, m, ndays)
            delta = d2 - d1

            return [str((d1 + timedelta(days=i)).strftime('%Y-%m-%d')) + " 00:00:00" for i in range(delta.days + 1)]
    except Exception as e:
        print(e)
        return ""

def get_days_for_month(month):
    try:
        m = month
        y = datetime.now().year
        if m == 12:
            ndays = (date(y+1, 1, 1) - date(y, m, 1)).days
            d1 = date(y, m, 1)
            d2 = date(y, m, ndays)
            delta = d2 - d1

            return [str((d1 + timedelta(days=i)).strftime('%Y-%m-%d')) + " 00:00:00" for i in range(delta.days + 1)]
        else:
            ndays = (date(y, m+1, 1) - date(y, m, 1)).days
            d1 = date(y, m, 1)
            d2 = date(y, m, ndays)
            delta = d2 - d1

            return [str((d1 + timedelta(days=i)).strftime('%Y-%m-%d')) + " 00:00:00" for i in range(delta.days + 1)]
    except Exception as e:
        print(e)
        return ""