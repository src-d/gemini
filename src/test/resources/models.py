from __future__ import unicode_literals

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.query import get_prefetcher


class Tag(models.Model):
    name = models.CharField(max_length=250, unique=True)

    def __unicode__(self):
        return self.name

    def __json__(self):
        return {
            'id': self.id,
            'name': self.name,
        }


class Post(models.Model):
    name = models.CharField(max_length=250)
    text = models.TextField(blank=True, default='')
    tags = models.ManyToManyField(Tag, related_name="posts")

    def __unicode__(self):
        return self.name

    def __json__(self):
        # small DB optimization, just to show that internals aren't scary
        try:
            _, _, _, is_fetched = get_prefetcher(self, 'tags', 'tags')
        except:
            is_fetched = False

        if is_fetched:
            tags = [i.name for i in self.tags.all()]
        else:
            tags = list(self.tags.values_list('name', flat=True))

        return {
            'id': self.id,
            'name': self.name,
            'text': self.text,
            'tags': tags
        }

    def tags_string(self):
        """ Return tags as a string for admin """

        return ', '.join(self.tags.values_list('name', flat=True))
