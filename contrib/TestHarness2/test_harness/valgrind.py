import enum
import xml
import xml.sax.handler
from pathlib import Path
from typing import List


class ValgrindWhat:
    def __init__(self):
        self.what: str = ''
        self.backtrace: str = ''


class ValgrindError:
    def __init__(self):
        self.what: ValgrindWhat = ValgrindWhat()
        self.kind: str = ''
        self.aux: List[ValgrindWhat] = []


# noinspection PyArgumentList
class ValgrindParseState(enum.Enum):
    ROOT = enum.auto()
    ERROR = enum.auto()
    ERROR_AUX = enum.auto()
    KIND = enum.auto()
    WHAT = enum.auto()
    TRACE = enum.auto()
    AUX_WHAT = enum.auto()
    STACK = enum.auto()
    STACK_AUX = enum.auto()
    STACK_IP = enum.auto()
    STACK_IP_AUX = enum.auto()


class ValgrindHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
        super().__init__()
        self.stack: List[ValgrindError] = []
        self.result: List[ValgrindError] = []
        self.state_stack: List[ValgrindParseState] = []

    def state(self) -> ValgrindParseState:
        if len(self.state_stack) == 0:
            return ValgrindParseState.ROOT
        return self.state_stack[-1]

    @staticmethod
    def from_content(content):
        # pdb.set_trace()
        if isinstance(content, bytes):
            return content.decode()
        assert isinstance(content, str)
        return content

    def characters(self, content):
        # pdb.set_trace()
        state = self.state()
        if len(self.state_stack) == 0:
            return
        else:
            assert len(self.stack) > 0
        if state is ValgrindParseState.KIND:
            self.stack[-1].kind += self.from_content(content)
        elif state is ValgrindParseState.WHAT:
            self.stack[-1].what.what += self.from_content(content)
        elif state is ValgrindParseState.AUX_WHAT:
            self.stack[-1].aux[-1].what += self.from_content(content)
        elif state is ValgrindParseState.STACK_IP:
            self.stack[-1].what.backtrace += self.from_content(content)
        elif state is ValgrindParseState.STACK_IP_AUX:
            self.stack[-1].aux[-1].backtrace += self.from_content(content)

    def startElement(self, name, attrs):
        # pdb.set_trace()
        if name == 'error':
            self.stack.append(ValgrindError())
            self.state_stack.append(ValgrindParseState.ERROR)
        if len(self.stack) == 0:
            return
        if name == 'kind':
            self.state_stack.append(ValgrindParseState.KIND)
        elif name == 'what':
            self.state_stack.append(ValgrindParseState.WHAT)
        elif name == 'auxwhat':
            assert self.state() in [ValgrindParseState.ERROR, ValgrindParseState.ERROR_AUX]
            self.state_stack.pop()
            self.state_stack.append(ValgrindParseState.ERROR_AUX)
            self.state_stack.append(ValgrindParseState.AUX_WHAT)
            self.stack[-1].aux.append(ValgrindWhat())
        elif name == 'stack':
            state = self.state()
            assert state in [ValgrindParseState.ERROR, ValgrindParseState.ERROR_AUX]
            if state == ValgrindParseState.ERROR:
                self.state_stack.append(ValgrindParseState.STACK)
            else:
                self.state_stack.append(ValgrindParseState.STACK_AUX)
        elif name == 'ip':
            state = self.state()
            assert state in [ValgrindParseState.STACK, ValgrindParseState.STACK_AUX]
            if state == ValgrindParseState.STACK:
                self.state_stack.append(ValgrindParseState.STACK_IP)
                if len(self.stack[-1].what.backtrace) == 0:
                    self.stack[-1].what.backtrace = 'addr2line -e fdbserver.debug -p -C -f -i '
                else:
                    self.stack[-1].what.backtrace += ' '
            else:
                self.state_stack.append(ValgrindParseState.STACK_IP_AUX)
                if len(self.stack[-1].aux[-1].backtrace) == 0:
                    self.stack[-1].aux[-1].backtrace = 'addr2line -e fdbserver.debug -p -C -f -i '
                else:
                    self.stack[-1].aux[-1].backtrace += ' '

    def endElement(self, name):
        # pdb.set_trace()
        if name == 'error':
            self.result.append(self.stack.pop())
            self.state_stack.pop()
        elif name == 'kind':
            assert self.state() == ValgrindParseState.KIND
            self.state_stack.pop()
        elif name == 'what':
            assert self.state() == ValgrindParseState.WHAT
            self.state_stack.pop()
        elif name == 'auxwhat':
            assert self.state() == ValgrindParseState.AUX_WHAT
            self.state_stack.pop()
        elif name == 'stack':
            assert self.state() in [ValgrindParseState.STACK, ValgrindParseState.STACK_AUX]
            self.state_stack.pop()
        elif name == 'ip':
            self.state_stack.pop()
            state = self.state()
            assert state in [ValgrindParseState.STACK, ValgrindParseState.STACK_AUX]


def parse_valgrind_output(valgrind_out_file: Path) -> List[ValgrindError]:
    handler = ValgrindHandler()
    with valgrind_out_file.open('r') as f:
        xml.sax.parse(f, handler)
        return handler.result
