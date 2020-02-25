# -*- coding: utf-8 -*-
"""
    sphinx.domains.ruby
    ~~~~~~~~~~~~~~~~~~~

    The Ruby domain.

    :copyright: Copyright 2010 by SHIBUKAWA Yoshiki
    :license: BSD

    Copyright (c) 2010 by SHIBUKAWA Yoshiki.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import re

from docutils import nodes
from docutils.parsers.rst import directives, Directive

from sphinx import addnodes
from sphinx.roles import XRefRole
from sphinx.locale import l_, _
from sphinx.domains import Domain, ObjType, Index
from sphinx.directives import ObjectDescription
from sphinx.util.nodes import make_refnode
from sphinx.util.docfields import Field, GroupedField, TypedField


# REs for Ruby signatures
rb_sig_re = re.compile(
    r'''^ ([\w.]*\.)?            # class name(s)
          (\$?\w+\??!?)  \s*     # thing name
          (?: \((.*)\)           # optional: arguments
           (?:\s* -> \s* (.*))?  #           return annotation
          )? $                   # and nothing more
          ''', re.VERBOSE)

rb_paramlist_re = re.compile(r'([\[\],])')  # split at '[', ']' and ','

separators = {
  'method':'#', 'attr_reader':'#', 'attr_writer':'#', 'attr_accessor':'#',
  'function':'.', 'classmethod':'.', 'class':'::', 'module':'::',
  'global':'', 'const':'::'}

rb_separator = re.compile(r"(?:\w+)?(?:::)?(?:\.)?(?:#)?")


def ruby_rsplit(fullname):
    items = [item for item in rb_separator.findall(fullname)]
    return ''.join(items[:-2]), items[-1]


class RubyObject(ObjectDescription):
    """
    Description of a general Ruby object.
    """
    option_spec = {
        'noindex': directives.flag,
        'module': directives.unchanged,
    }

    doc_field_types = [
        TypedField('parameter', label=l_('Parameters'),
                   names=('param', 'parameter', 'arg', 'argument'),
                   typerolename='obj', typenames=('paramtype', 'type')),
        TypedField('variable', label=l_('Variables'), rolename='obj',
                   names=('var', 'ivar', 'cvar'),
                   typerolename='obj', typenames=('vartype',)),
        GroupedField('exceptions', label=l_('Raises'), rolename='exc',
                     names=('raises', 'raise', 'exception', 'except'),
                     can_collapse=True),
        Field('returnvalue', label=l_('Returns'), has_arg=False,
              names=('returns', 'return')),
        Field('returntype', label=l_('Return type'), has_arg=False,
              names=('rtype',)),
    ]

    def get_signature_prefix(self, sig):
        """
        May return a prefix to put before the object name in the signature.
        """
        return ''

    def needs_arglist(self):
        """
        May return true if an empty argument list is to be generated even if
        the document contains none.
        """
        return False

    def handle_signature(self, sig, signode):
        """
        Transform a Ruby signature into RST nodes.
        Returns (fully qualified name of the thing, classname if any).

        If inside a class, the current class name is handled intelligently:
        * it is stripped from the displayed name if present
        * it is added to the full name (return value) if not present
        """
        m = rb_sig_re.match(sig)
        if m is None:
            raise ValueError
        name_prefix, name, arglist, retann = m.groups()
        if not name_prefix:
            name_prefix = ""
        # determine module and class name (if applicable), as well as full name
        modname = self.options.get(
            'module', self.env.temp_data.get('rb:module'))
        classname = self.env.temp_data.get('rb:class')
        if self.objtype == 'global':
            add_module = False
            modname = None
            classname = None
            fullname = name
        elif classname:
            add_module = False
            if name_prefix and name_prefix.startswith(classname):
                fullname = name_prefix + name
                # class name is given again in the signature
                name_prefix = name_prefix[len(classname):].lstrip('.')
            else:
                separator = separators[self.objtype]
                fullname = classname + separator + name_prefix + name
        else:
            add_module = True
            if name_prefix:
                classname = name_prefix.rstrip('.')
                fullname = name_prefix + name
            else:
                classname = ''
                fullname = name

        signode['module'] = modname
        signode['class'] = self.class_name = classname
        signode['fullname'] = fullname

        sig_prefix = self.get_signature_prefix(sig)
        if sig_prefix:
            signode += addnodes.desc_annotation(sig_prefix, sig_prefix)

        if name_prefix:
            signode += addnodes.desc_addname(name_prefix, name_prefix)
        # exceptions are a special case, since they are documented in the
        # 'exceptions' module.
        elif add_module and self.env.config.add_module_names:
            if self.objtype == 'global':
                nodetext = ''
                signode += addnodes.desc_addname(nodetext, nodetext)
            else:
                modname = self.options.get(
                    'module', self.env.temp_data.get('rb:module'))
                if modname and modname != 'exceptions':
                    nodetext = modname + separators[self.objtype]
                    signode += addnodes.desc_addname(nodetext, nodetext)

        signode += addnodes.desc_name(name, name)
        if not arglist:
            if self.needs_arglist():
                # for callables, add an empty parameter list
                signode += addnodes.desc_parameterlist()
            if retann:
                signode += addnodes.desc_returns(retann, retann)
            return fullname, name_prefix
        signode += addnodes.desc_parameterlist()

        stack = [signode[-1]]
        for token in rb_paramlist_re.split(arglist):
            if token == '[':
                opt = addnodes.desc_optional()
                stack[-1] += opt
                stack.append(opt)
            elif token == ']':
                try:
                    stack.pop()
                except IndexError:
                    raise ValueError
            elif not token or token == ',' or token.isspace():
                pass
            else:
                token = token.strip()
                stack[-1] += addnodes.desc_parameter(token, token)
        if len(stack) != 1:
            raise ValueError
        if retann:
            signode += addnodes.desc_returns(retann, retann)
        return fullname, name_prefix

    def get_index_text(self, modname, name):
        """
        Return the text for the index entry of the object.
        """
        raise NotImplementedError('must be implemented in subclasses')

    def _is_class_member(self):
        return self.objtype.endswith('method') or self.objtype.startswith('attr')

    def add_target_and_index(self, name_cls, sig, signode):
        if self.objtype == 'global':
            modname = ''
        else:
            modname = self.options.get(
                'module', self.env.temp_data.get('rb:module'))
        separator = separators[self.objtype]
        if self._is_class_member():
            if signode['class']:
                prefix = modname and modname + '::' or ''
            else:
                prefix = modname and modname + separator or ''
        else:
            prefix = modname and modname + separator or ''
        fullname = prefix + name_cls[0]
        # note target
        if fullname not in self.state.document.ids:
            signode['names'].append(fullname)
            signode['ids'].append(fullname)
            signode['first'] = (not self.names)
            self.state.document.note_explicit_target(signode)
            objects = self.env.domaindata['rb']['objects']
            if fullname in objects:
                self.env.warn(
                    self.env.docname,
                    'duplicate object description of %s, ' % fullname +
                    'other instance in ' +
                    self.env.doc2path(objects[fullname][0]),
                    self.lineno)
            objects[fullname] = (self.env.docname, self.objtype)

        indextext = self.get_index_text(modname, name_cls)
        if indextext:
            self.indexnode['entries'].append(('single', indextext,
                                              fullname, fullname, None))

    def before_content(self):
        # needed for automatic qualification of members (reset in subclasses)
        self.clsname_set = False

    def after_content(self):
        if self.clsname_set:
            self.env.temp_data['rb:class'] = None


class RubyModulelevel(RubyObject):
    """
    Description of an object on module level (functions, data).
    """

    def needs_arglist(self):
        return self.objtype == 'function'

    def get_index_text(self, modname, name_cls):
        if self.objtype == 'function':
            if not modname:
                return _('%s() (global function)') % name_cls[0]
            return _('%s() (module function in %s)') % (name_cls[0], modname)
        else:
            return ''


class RubyGloballevel(RubyObject):
    """
    Description of an object on module level (functions, data).
    """

    def get_index_text(self, modname, name_cls):
        if self.objtype == 'global':
            return _('%s (global variable)') % name_cls[0]
        else:
            return ''


class RubyEverywhere(RubyObject):
    """
    Description of a class member (methods, attributes).
    """

    def needs_arglist(self):
        return self.objtype == 'method'

    def get_index_text(self, modname, name_cls):
        name, cls = name_cls
        add_modules = self.env.config.add_module_names
        if self.objtype == 'method':
            try:
                clsname, methname = ruby_rsplit(name)
            except ValueError:
                if modname:
                    return _('%s() (in module %s)') % (name, modname)
                else:
                    return '%s()' % name
            if modname and add_modules:
                return _('%s() (%s::%s method)') % (methname, modname,
                                                          clsname)
            else:
                return _('%s() (%s method)') % (methname, clsname)
        else:
            return ''


class RubyClasslike(RubyObject):
    """
    Description of a class-like object (classes, exceptions).
    """

    def get_signature_prefix(self, sig):
        return self.objtype + ' '

    def get_index_text(self, modname, name_cls):
        if self.objtype == 'class':
            if not modname:
                return _('%s (class)') % name_cls[0]
            return _('%s (class in %s)') % (name_cls[0], modname)
        elif self.objtype == 'exception':
            return name_cls[0]
        else:
            return ''

    def before_content(self):
        RubyObject.before_content(self)
        if self.names:
            self.env.temp_data['rb:class'] = self.names[0][0]
            self.clsname_set = True


class RubyClassmember(RubyObject):
    """
    Description of a class member (methods, attributes).
    """

    def needs_arglist(self):
        return self.objtype.endswith('method')

    def get_signature_prefix(self, sig):
        if self.objtype == 'classmethod':
            return "classmethod %s." % self.class_name
        elif self.objtype == 'attr_reader':
            return "attribute [R] "
        elif self.objtype == 'attr_writer':
            return "attribute [W] "
        elif self.objtype == 'attr_accessor':
            return "attribute [R/W] "
        return ''

    def get_index_text(self, modname, name_cls):
        name, cls = name_cls
        add_modules = self.env.config.add_module_names
        if self.objtype == 'classmethod':
            try:
                clsname, methname = ruby_rsplit(name)
            except ValueError:
                return '%s()' % name
            if modname:
                return _('%s() (%s.%s class method)') % (methname, modname,
                                                         clsname)
            else:
                return _('%s() (%s class method)') % (methname, clsname)
        elif self.objtype.startswith('attr'):
            try:
                clsname, attrname = ruby_rsplit(name)
            except ValueError:
                return name
            if modname and add_modules:
                return _('%s (%s.%s attribute)') % (attrname, modname, clsname)
            else:
                return _('%s (%s attribute)') % (attrname, clsname)
        else:
            return ''

    def before_content(self):
        RubyObject.before_content(self)
        lastname = self.names and self.names[-1][1]
        if lastname and not self.env.temp_data.get('rb:class'):
            self.env.temp_data['rb:class'] = lastname.strip('.')
            self.clsname_set = True


class RubyModule(Directive):
    """
    Directive to mark description of a new module.
    """

    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {
        'platform': lambda x: x,
        'synopsis': lambda x: x,
        'noindex': directives.flag,
        'deprecated': directives.flag,
    }

    def run(self):
        env = self.state.document.settings.env
        modname = self.arguments[0].strip()
        noindex = 'noindex' in self.options
        env.temp_data['rb:module'] = modname
        env.domaindata['rb']['modules'][modname] = \
            (env.docname, self.options.get('synopsis', ''),
             self.options.get('platform', ''), 'deprecated' in self.options)
        targetnode = nodes.target('', '', ids=['module-' + modname], ismod=True)
        self.state.document.note_explicit_target(targetnode)
        ret = [targetnode]
        # XXX this behavior of the module directive is a mess...
        if 'platform' in self.options:
            platform = self.options['platform']
            node = nodes.paragraph()
            node += nodes.emphasis('', _('Platforms: '))
            node += nodes.Text(platform, platform)
            ret.append(node)
        # the synopsis isn't printed; in fact, it is only used in the
        # modindex currently
        if not noindex:
            indextext = _('%s (module)') % modname
            inode = addnodes.index(entries=[('single', indextext,
                                             'module-' + modname, modname, None)])
            ret.append(inode)
        return ret


class RubyCurrentModule(Directive):
    """
    This directive is just to tell Sphinx that we're documenting
    stuff in module foo, but links to module foo won't lead here.
    """

    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {}

    def run(self):
        env = self.state.document.settings.env
        modname = self.arguments[0].strip()
        if modname == 'None':
            env.temp_data['rb:module'] = None
        else:
            env.temp_data['rb:module'] = modname
        return []


class RubyXRefRole(XRefRole):
    def process_link(self, env, refnode, has_explicit_title, title, target):
        if not has_explicit_title:
            title = title.lstrip('.')   # only has a meaning for the target
            title = title.lstrip('#')
            if title.startswith("::"):
                title = title[2:]
            target = target.lstrip('~') # only has a meaning for the title
            # if the first character is a tilde, don't display the module/class
            # parts of the contents
            if title[0:1] == '~':
                m = re.search(r"(?:\.)?(?:#)?(?:::)?(.*)\Z", title)
                if m:
                    title = m.group(1)
        if not title.startswith("$"):
            refnode['rb:module'] = env.temp_data.get('rb:module')
            refnode['rb:class'] = env.temp_data.get('rb:class')
        # if the first character is a dot, search more specific namespaces first
        # else search builtins first
        if target[0:1] == '.':
            target = target[1:]
            refnode['refspecific'] = True
        return title, target


class RubyModuleIndex(Index):
    """
    Index subclass to provide the Ruby module index.
    """

    name = 'modindex'
    localname = l_('Ruby Module Index')
    shortname = l_('modules')

    def generate(self, docnames=None):
        content = {}
        # list of prefixes to ignore
        ignores = self.domain.env.config['modindex_common_prefix']
        ignores = sorted(ignores, key=len, reverse=True)
        # list of all modules, sorted by module name
        modules = sorted(iter(self.domain.data['modules'].items()),
                         key=lambda x: x[0].lower())
        # sort out collapsable modules
        prev_modname = ''
        num_toplevels = 0
        for modname, (docname, synopsis, platforms, deprecated) in modules:
            if docnames and docname not in docnames:
                continue

            for ignore in ignores:
                if modname.startswith(ignore):
                    modname = modname[len(ignore):]
                    stripped = ignore
                    break
            else:
                stripped = ''

            # we stripped the whole module name?
            if not modname:
                modname, stripped = stripped, ''

            entries = content.setdefault(modname[0].lower(), [])

            package = modname.split('::')[0]
            if package != modname:
                # it's a submodule
                if prev_modname == package:
                    # first submodule - make parent a group head
                    entries[-1][1] = 1
                elif not prev_modname.startswith(package):
                    # submodule without parent in list, add dummy entry
                    entries.append([stripped + package, 1, '', '', '', '', ''])
                subtype = 2
            else:
                num_toplevels += 1
                subtype = 0

            qualifier = deprecated and _('Deprecated') or ''
            entries.append([stripped + modname, subtype, docname,
                            'module-' + stripped + modname, platforms,
                            qualifier, synopsis])
            prev_modname = modname

        # apply heuristics when to collapse modindex at page load:
        # only collapse if number of toplevel modules is larger than
        # number of submodules
        collapse = len(modules) - num_toplevels < num_toplevels

        # sort by first letter
        content = sorted(content.items())

        return content, collapse


class RubyDomain(Domain):
    """Ruby language domain."""
    name = 'rb'
    label = 'Ruby'
    object_types = {
        'function':        ObjType(l_('function'),         'func', 'obj'),
        'global':          ObjType(l_('global variable'),  'global', 'obj'),
        'method':          ObjType(l_('method'),           'meth', 'obj'),
        'class':           ObjType(l_('class'),            'class', 'obj'),
        'exception':       ObjType(l_('exception'),        'exc', 'obj'),
        'classmethod':     ObjType(l_('class method'),     'meth', 'obj'),
        'attr_reader':     ObjType(l_('attribute'),        'attr', 'obj'),
        'attr_writer':     ObjType(l_('attribute'),        'attr', 'obj'),
        'attr_accessor':   ObjType(l_('attribute'),        'attr', 'obj'),
        'const':           ObjType(l_('const'),            'const', 'obj'),
        'module':          ObjType(l_('module'),           'mod', 'obj'),
    }

    directives = {
        'function':        RubyModulelevel,
        'global':          RubyGloballevel,
        'method':          RubyEverywhere,
        'const':           RubyEverywhere,
        'class':           RubyClasslike,
        'exception':       RubyClasslike,
        'classmethod':     RubyClassmember,
        'attr_reader':     RubyClassmember,
        'attr_writer':     RubyClassmember,
        'attr_accessor':   RubyClassmember,
        'module':          RubyModule,
        'currentmodule':   RubyCurrentModule,
    }

    roles = {
        'func':  RubyXRefRole(fix_parens=False),
        'global':RubyXRefRole(),
        'class': RubyXRefRole(),
        'exc':   RubyXRefRole(),
        'meth':  RubyXRefRole(fix_parens=False),
        'attr':  RubyXRefRole(),
        'const': RubyXRefRole(),
        'mod':   RubyXRefRole(),
        'obj':   RubyXRefRole(),
    }
    initial_data = {
        'objects': {},  # fullname -> docname, objtype
        'modules': {},  # modname -> docname, synopsis, platform, deprecated
    }
    indices = [
        RubyModuleIndex,
    ]

    def clear_doc(self, docname):
        for fullname, (fn, _) in list(self.data['objects'].items()):
            if fn == docname:
                del self.data['objects'][fullname]
        for modname, (fn, _, _, _) in list(self.data['modules'].items()):
            if fn == docname:
                del self.data['modules'][modname]

    def find_obj(self, env, modname, classname, name, type, searchorder=0):
        """
        Find a Ruby object for "name", perhaps using the given module and/or
        classname.
        """
        # skip parens
        if name[-2:] == '()':
            name = name[:-2]

        if not name:
            return None, None

        objects = self.data['objects']

        newname = None
        if searchorder == 1:
            if modname and classname and \
                     modname + '::' + classname + '#' + name in objects:
                newname = modname + '::' + classname + '#' + name
            elif modname and classname and \
                     modname + '::' + classname + '.' + name in objects:
                newname = modname + '::' + classname + '.' + name
            elif modname and modname + '::' + name in objects:
                newname = modname + '::' + name
            elif modname and modname + '#' + name in objects:
                newname = modname + '#' + name
            elif modname and modname + '.' + name in objects:
                newname = modname + '.' + name
            elif classname and classname + '.' + name in objects:
                newname = classname + '.' + name
            elif classname and classname + '#' + name in objects:
                newname = classname + '#' + name
            elif name in objects:
                newname = name
        else:
            if name in objects:
                newname = name
            elif classname and classname + '.' + name in objects:
                newname = classname + '.' + name
            elif classname and classname + '#' + name in objects:
                newname = classname + '#' + name
            elif modname and modname + '::' + name in objects:
                newname = modname + '::' + name
            elif modname and modname + '#' + name in objects:
                newname = modname + '#' + name
            elif modname and modname + '.' + name in objects:
                newname = modname + '.' + name
            elif modname and classname and \
                     modname + '::' + classname + '#' + name in objects:
                newname = modname + '::' + classname + '#' + name
            elif modname and classname and \
                     modname + '::' + classname + '.' + name in objects:
                newname = modname + '::' + classname + '.' + name
            # special case: object methods
            elif type in ('func', 'meth') and '.' not in name and \
                 'object.' + name in objects:
                newname = 'object.' + name
        if newname is None:
            return None, None
        return newname, objects[newname]

    def resolve_xref(self, env, fromdocname, builder,
                     typ, target, node, contnode):
        if (typ == 'mod' or
            typ == 'obj' and target in self.data['modules']):
            docname, synopsis, platform, deprecated = \
                self.data['modules'].get(target, ('','','', ''))
            if not docname:
                return None
            else:
                title = '%s%s%s' % ((platform and '(%s) ' % platform),
                                    synopsis,
                                    (deprecated and ' (deprecated)' or ''))
                return make_refnode(builder, fromdocname, docname,
                                    'module-' + target, contnode, title)
        else:
            modname = node.get('rb:module')
            clsname = node.get('rb:class')
            searchorder = node.hasattr('refspecific') and 1 or 0
            name, obj = self.find_obj(env, modname, clsname,
                                      target, typ, searchorder)
            if not obj:
                return None
            else:
                return make_refnode(builder, fromdocname, obj[0], name,
                                    contnode, name)

    def get_objects(self):
        for modname, info in self.data['modules'].items():
            yield (modname, modname, 'module', info[0], 'module-' + modname, 0)
        for refname, (docname, type) in self.data['objects'].items():
            yield (refname, refname, type, docname, refname, 1)


def setup(app):
    app.add_domain(RubyDomain)
