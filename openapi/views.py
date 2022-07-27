from django.shortcuts import render

# Create your views here.
def index(request):
    return render(request, 'openapi/index.html')


def gotoHTML(request, html):
    return render(request, 'openapi/'+html)