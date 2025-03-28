This page contains resources that may be helpful for learning to use `earthmover`. Using `earthmover` will be easier if you understand these topics.

## YAML
YAML is a popular format for storing data in various structures. It is functionally equivalent to JSON.

[This video](https://www.youtube.com/watch?v=BEki_rsWu4E) is a good introduction to [YAML](https://yaml.org/). For `earthmover`, it is helpful to be familiar with comments, basic data types, lists and dictionaries, nesting, and [anchors, aliases, and overrides](https://www.linode.com/docs/guides/yaml-anchors-aliases-overrides-extensions/). 

???+ tip "Did you know?"
    YAML was chosen as the configuration language for `earthmover` because it's less verbose than XML or JSON, and it supports comments!

```yaml title="YAML example"
# YAML supports comments!

# scalar values
my_boolean: True # or False
my_integer: 47
my_float: 3.14
my_string: string_value
another_string: "Another string value!"

# lists (arrays)
my_small_number_array: [1, 2, 3]
my_large_quote_array:
  - "The only thing we have to fear is fear itself"
  - "I think, therefore I am"
  - "Simplicity is the ultimate sophistication"
  - "In the middle of every difficulty lies opportunity"
  - "The secret of getting ahead is getting started"
  - "There is no substitute for hard work"
  - "Whatever you do, do it well"
  - "The best way to predict the future is to create it"

# dictionaries (objects)
my_car:
  make: Subaru
  model: Outback
  year: 2015
  color: blue
  plate: N0 WAY

# lists of objects
my_pets:
- type: dog
  name: Snoopy
  age: 7
  is_good_boy: True
- type: cat
  name: Garfield
  age: 6
  has_attitude: True
```

## Jinja templates
[This video](https://youtu.be/4yaG-jFfePc?si=HMlBLHv002dfPQzR&t=37) is a good introduction to [Jinja templates](https://jinja.palletsprojects.com/en/stable/templates/). For `earthmover`, it is helpful to understand Jinja [comments](https://jinja.palletsprojects.com/en/stable/templates/#comments), [variables](https://jinja.palletsprojects.com/en/stable/templates/#variables), control structures like [if](https://jinja.palletsprojects.com/en/stable/templates/#if) and [for](https://jinja.palletsprojects.com/en/stable/templates/#for), [filters](https://jinja.palletsprojects.com/en/stable/templates/#list-of-builtin-filters), and [macros](https://jinja.palletsprojects.com/en/stable/templates/#macros).

???+ tip "Did you know?"
    Jinja and YAML have emerged as de-facto languages in data engineering and other engineering disciplines. [`ansible`](https://github.com/ansible/ansible) (a devops tool) uses YAML that may contain Jinja, just like `earthmover`. [`dbt`](https://github.com/dbt-labs/dbt-core) uses plain YAML for its project configuration with Jinja in the SQL models it runs.

```jinja title="Jinja example"
{# Jinja supports comments! #}

Hello {{user.name}}! You were born in {{user.birth_date[0:4]}}.

{% if user.pets | length > 0 %}
  Your pets include:
  {% for pet in user.pets %}
    * {{pet.name}} ({{pet.age}}) {% if pet.is_good_boy %}🐶{% endif %}
  {% endfor %}
  {% if user.pets | selectattr("has_attitude") | length > 0 %}
    Some of your pets have attitude!
  {% endif %}
{% else %}
  You have no pets 😞 (yet!)
{% endif %}


```

<!-- Data joins? -->
