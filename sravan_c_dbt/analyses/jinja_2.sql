{% set apples = ["Gala","Red Delicious","Fuji","McIntosh","Honeycrisp"] %}

{% for i in apples %}
    {% if i != "McIntosh" %}
        {{ i }}
    {% else %}
        {{ i }} " is my favorite apple!" 
    
    {% endif %}
    
{% endfor %}


